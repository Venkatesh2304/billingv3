import datetime
import os
import threading
import time
import traceback
from PyPDF2 import PdfMerger
from django.http import JsonResponse
import pandas as pd
from enum import Enum, IntEnum
import app.models as models 
from app.sync import sync_reports
from custom import secondarybills
from custom.classes import Billing, Einvoice
from django.db.models import F,F
from rest_framework.decorators import api_view
import app.pdf_create as pdf_create
import app.aztec as aztec 
import hashlib
import json
from django.http import JsonResponse

#TODO : ENUMS 
class BillingLock : 
    def __init__(self) :
        self.lock = threading.Lock()
        self.last_locked_time = None

    def acquire(self) :
        self.lock.acquire()
        self.last_locked_time = datetime.datetime.now()

    def release(self) :
        self.lock.release()
        self.last_locked_time = None

    def locked(self) :
        return self.lock.locked()
    
    def locked_too_long(self) :
        if self.last_locked_time :
            return (datetime.datetime.now() - self.last_locked_time).seconds > 600
        return False
             
class BillingStatus(IntEnum) :
    NotStarted = 0
    Success = 1
    Started = 2
    Failed = 3

billing_process_names = ("SYNC" , "PREVBILLS" , "RELEASELOCK" , "COLLECTION", "ORDER" , "DELIVERY", "REPORTS")
billing_lock = BillingLock() 

@api_view(["GET","POST"])
def billing_view(request) :
    data = request.data
    order_date = data.get("order_date") or datetime.date.today()
    last_billing = models.Billing.objects.filter(start_time__gte = datetime.datetime.now()  - datetime.timedelta(minutes=90)
                                                            ).order_by("-start_time").first()
    last_billing_id = last_billing.id if last_billing else None
    if request.method == "GET" :
        return JsonResponse({"billing_id" : last_billing_id })
    
    billing_id = data.get("billing_id")
    if billing_lock.locked() :
        if billing_lock.locked_too_long():
            billing_lock.release()
        else :
            return JsonResponse({ "billing_id" : last_billing_id  , "error" : "Someone is already running the billing process" })
    
    if last_billing_id and (billing_id != last_billing_id) : 
        return JsonResponse({ "billing_id" : last_billing_id , "error" : "This Billing is Old" })

    billing_lock.acquire()
    #Create Billing & Status Log in DB
    billing_log = models.Billing(start_time = datetime.datetime.now(), status = BillingStatus.Started, 
                                        date = order_date)
    billing_log.save()
    for process_name in billing_process_names :
        models.BillingProcessStatus(billing = billing_log,process = process_name,status = BillingStatus.NotStarted).save()
    
    thread = threading.Thread( target = run_billing_process , args = (billing_log,data) )
    thread.start() 
    return JsonResponse({"billing_id" : billing_log.id })
    
def run_billing_process(billing_log,data) :

    ##Calculate the neccesary values for the billing
    today = datetime.date.today()
    max_lines = data.get("max_lines")    
    order_date =  datetime.datetime.strptime(data.get("order_date"),"%Y-%m-%d").date()

    prev_order_total_values = { order.order_no : order.bill_value for order in models.Orders.objects.filter(date = order_date) }
    
    delete_order_nos = [ order_no for order_no, selected in data.get("delete").items() if selected ]
    forced_order_nos = [ order_no for order_no, selected in data.get("force_place").items() if selected ]
    
    old_billing_id = data.get("billing_id")
    if old_billing_id :
        old_billing = models.Billing.objects.get(id = old_billing_id)
        last_billing_orders = models.Orders.objects.filter(billing = old_billing)
        creditrelease = list(last_billing_orders.filter(creditlock=True,order_no__in = forced_order_nos))
        creditrelease = pd.DataFrame([ [order.party_id , order.party_id , order.party.hul_code ,order.beat.plg.replace('+','%2B')] for order in creditrelease ] , # type: ignore
                                    columns=["partyCode","parCodeRef","parHllCode","showPLG"])
        creditrelease = creditrelease.groupby(["partyCode","parCodeRef","parHllCode","showPLG"]).size().reset_index(name='increase_count') # type: ignore
        creditrelease = creditrelease.to_dict(orient="records")
    else : 
        creditrelease = []
     
    def filter_orders_fn(order: pd.Series) : 
        return (((today == order_date) or (order.iloc[0].ot == "SH")) and all([
              order.on.count() <= max_lines ,
              (order.on.iloc[0] not in prev_order_total_values) or abs((order.t * order.cq).sum() - prev_order_total_values[order.on.iloc[0]]) <= 1 , 
              "WHOLE" not in order.m.iloc[0] ,
              (order.t * order.cq).sum() >= 200
            ])) or (order.on.iloc[0] in forced_order_nos)

    ##Intiate the Ikea Billing Session
    order_objects:list[models.Orders] = []
    try :  
        billing = Billing(order_date = order_date,filter_orders_fn = filter_orders_fn)
    except Exception as e: 
        print("Billing Session Failed\n" , traceback.format_exc() )
        billing_log.error = str(traceback.format_exc())
        billing_log.status = BillingStatus.Failed
        billing_log.save()
        sync_process_obj = models.BillingProcessStatus.objects.get(billing=billing_log,process="SYNC")
        sync_process_obj.status = BillingStatus.Failed
        sync_process_obj.save()
        billing_lock.release()
        return
    
    ##Functions combing Ikea Session + Database 
    def PrevDeliveryProcess() : 
        billing.Prevbills()
        models.Sales.objects.filter(inum__in = billing.prevbills).update(delivered = False)

    def CollectionProcess() : 
        billing.Collection()
        models.PushedCollection.objects.bulk_create([ models.PushedCollection(
                   billing = billing_log, party_code = pc) for pc in billing.pushed_collection_party_ids ])
        
    def OrderProcess() : 
        billing.Order(delete_order_nos)
        last_billing_orders = billing.all_orders    
        if len(last_billing_orders.index) == 0 : return 

        models.Party.objects.bulk_create([ 
            models.Party( name = row.p ,code = row.pc ) 
            for _,row in last_billing_orders.drop_duplicates(subset="pc").iterrows() ],
         update_conflicts=True,
         unique_fields=['code'],
         update_fields=["name"])
        filtered_orders = billing.filtered_orders.on.values
        
        ## Warning add and condition 
        order_objects.extend( models.Orders.objects.bulk_create([ 
            models.Orders( order_no=row.on,party_id = row.pc,salesman=row.s, 
                    creditlock = ("Credit Exceeded" in row.ar) , place_order = (row.on in filtered_orders) , 
                beat_id = row.mi , billing = billing_log , date = datetime.datetime.now().date() , type = row.ot   ) 
            for _,row in last_billing_orders.drop_duplicates(subset="on").iterrows() ],
         update_conflicts=True,
         unique_fields=['order_no'],
         update_fields=["billing_id","type","creditlock","place_order"]) )
        

        models.OrderProducts.objects.filter(order__in = order_objects,allocated = 0).update(allocated = F("quantity"),reason = "Guessed allocation")
        models.OrderProducts.objects.bulk_create([ models.OrderProducts(
            order_id=row.on,product=row.bd,batch=row.bc,quantity=row.cq,allocated = row.aq,rate = row.t,reason = row.ar) for _,row in last_billing_orders.iterrows() ] , 
         update_conflicts=True,
         unique_fields=['order_id','product','batch'],
         update_fields=['quantity','rate','allocated','reason'])

    def ReportProcess() :
        sync_reports(billing,limits={"sales" : None , "adjustment" : None , "collection" : None })
        models.Sales.objects.filter(inum__in = billing.prevbills).update(delivered = False)
                      
    def DeliveryProcess() : 
        billing.Delivery()
        if len(billing.bills) == 0 : return 
        billing_log.start_bill_no = billing.bills[0]
        billing_log.end_bill_no = billing.bills[-1]
        billing_log.bill_count = len(billing.bills)
        billing_log.save()

    ##Start the proccess
    billing_process_functions = [billing.Sync , PrevDeliveryProcess ,  (lambda : billing.release_creditlocks(creditrelease)) , 
                                  CollectionProcess ,  OrderProcess ,  DeliveryProcess , ReportProcess  ]
    billing_process =  dict(zip(billing_process_names,billing_process_functions)) 
    billing_failed = False 
    for process_name,process in billing_process.items() : 
        process_obj = models.BillingProcessStatus.objects.get(billing=billing_log,process=process_name)
        process_obj.status = BillingStatus.Started
        process_obj.save()    
        start_time = time.time()
        try : 
            process()          
            print("Completed " , process_name)    
        except Exception as e :
            traceback.print_exc()
            billing_log.error = str(traceback.format_exc())
            billing_failed = True 

        process_obj.status = (BillingStatus.Failed if billing_failed else  BillingStatus.Success)
        end_time = time.time()
        process_obj.time = round(end_time - start_time,2)
        process_obj.save()
        if billing_failed :  break 
        
    billing_log.end_time = datetime.datetime.now() 
    billing_log.status = BillingStatus.Failed if billing_failed else  BillingStatus.Success
    billing_log.save()
    billing_lock.release()

