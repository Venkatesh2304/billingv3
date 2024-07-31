from collections import defaultdict
import datetime
from io import BytesIO
import re
from threading import Thread
import threading
import time
import traceback
from typing import Any
from django.contrib import admin
from django.db.models.query import QuerySet
from django.http import HttpResponse
from django.http.request import HttpRequest
import numpy as np
import pandas as pd
import app.models as models 
from django.utils.html import format_html
from django.contrib.admin.templatetags.admin_list import register , result_list  
from django.contrib.admin.templatetags.base import InclusionAdminNode  
from custom.classes import Billing,IkeaDownloader
from django.utils import timezone
from django.db.models import Max,F,Subquery,OuterRef,Q,Min,Sum,Count
from django.contrib.admin import helpers, widgets
from collections import namedtuple

def user_permission(s,*a,**kw) : 
    if a and ("change" in a[0] or "delete" in a[0] or "add" in a[0] ) : return False #or "add" in a[0]
    return True

class AccessUser(object):
    has_module_perms = has_perm = __getattr__ = user_permission

admin_site = admin.AdminSite(name='myadmin')
admin_site.has_permission = lambda r: setattr(r, 'user', AccessUser()) or True

@register.tag(name="custom_result_list")
def result_list_tag(parser, token):
    return  InclusionAdminNode(
        parser,
        token,
        func=result_list,
        template_name="custom_result_list.html",
        takes_context=False,
    )

def remap_beats(beat_maps,pending_bills) : 
    pending = pending_bills.iloc[:-1].rename(columns = {"Beat Name" : "Beat"})
    def create_plg(df) :
        col_name = "Beat"
        df["plg"] = df[col_name].str.split(" ").str[0].str.split("-").str[0]
        df.plg = df.plg.apply(lambda x : x[:3] if "+" in x else x)
        df.plg = df.plg.replace("HUL+NUTS","HUL")
        df.plg = df.plg.str.replace("PP","P")
        df.plg = df.plg.replace({ "FNR" : "F+N" , "HUL" : "D+P+F+N" })
        df.plg = df.plg.str.replace("H","N")

    create_plg(pending)
    beat_maps = beat_maps[beat_maps["Beat"] != "UDB DUMMY BEAT"]
    create_plg(beat_maps)
    dfs = []

    for (hul_code,old_plg) , bills in pending.groupby(["Party HUL Code","plg"]) :
        maps = beat_maps[beat_maps["Party HUL Code"] == hul_code]
        if (len(maps.index) == 0) or (maps.iloc[0]["plg"] == "UDB") : pass
        new_plg = None
        for idx,map_row in maps.iterrows() :
            if old_plg in map_row["plg"] :
                new_plg = map_row["plg"]
                break
       
        if new_plg is None :
            if old_plg.count("+") == 0 : pass
            elif old_plg.count("+") == 1 :
                new_plg = old_plg.split("+")[0]
            elif old_plg.count("+") == 3 :
                new_plg = "D+P" if "D+P" in maps["plg"].values else "D"
            else :
                pass
             
        bills = bills.reset_index()
        bills["new_plg"] = new_plg
    
        if len(maps[ maps.plg == new_plg ].index) == 0 :
            new_beat = None
        else :
            new_beat = maps[ maps.plg == new_plg ].iloc[0]["Beat"]

        bills["new_beat"] = new_beat
        dfs.append( bills )


    df = pd.concat(dfs)
    df.loc[ pd.isna(df.new_beat) , "new_beat" ] = "UDB DUMMY BEAT"
    df.loc[ df.new_beat == "UDB DUMMY BEAT" , "new_plg" ] = "UDB"
    df["old_beat"] = df.Beat
    df["Beat"] = df.new_beat
    del df["new_plg"]
    del df["plg"]
    return df 

def outstanding() : 
    ikea = IkeaDownloader()
    date = datetime.datetime.now().date() + datetime.timedelta(days=1)
    beat_mapping = ikea.beat_mapping() 
    pending_bills = ikea.pending_bills(date)
    pending_bills = remap_beats(beat_mapping,pending_bills)
    html = ikea.get("/rsunify/app/rssmBeatPlgLink/loadRssmBeatPlgLink#!").text

    salesman_ids = re.findall(r"<input type=\"hidden\" value=\"([0-9]+)\" />",html,re.DOTALL)[::3] 
    salesman_names = pd.read_html(html)[0]["Salesperson Name"]
    salesman_maps = dict(zip(salesman_ids,salesman_names))

    salesman = ikea.post("/rsunify/app/paginationController/getPopScreenData", 
                        json = {"jasonParam":{ "viewName":"VIEW_LOAD_SALESMAN_BEAT_LINK_SALESMAN_LIST","pageNumber":1,"pageSize":200}} ).json() 
    sal_id = map( lambda x : x[1] , salesman[0][1:] )
    beats_data = []
    day = date.strftime('%A').lower() + "Linked"
    for id in sal_id : 
         beats_data += ikea.post("/rsunify/app/salesmanBeatLink/getSalesmanBeatLinkMappings",
              data={"divisionId": 0, "salesmanId": int(id) }).json()
    
    beats_data = pd.DataFrame(beats_data)
    filteredBeats = list(set(beats_data[beats_data[day] != '0']["beatName"]))

    pending_bills = pending_bills.merge(beats_data[["beatName","salesmanId"]],how="left",left_on="Beat",right_on="beatName")
    pending_bills["Salesperson"] = pending_bills.salesmanId.replace(salesman_maps)
    pending_bills.dropna(subset = ["Salesperson Name"] , inplace = True )
    pending_bills["party"] = pending_bills["Party Name"]
    del pending_bills["Salesperson Name"]

    today_pending_bills = pending_bills[pending_bills.Beat.isin(filteredBeats)]

    ## PENDING BILLS COLUMN
    BILL_NO = "Bill No"
    SALESMAN = "Salesperson"
    BEAT = "Beat"
    PARTY = "party"
    OS_AMT = "OutstANDing Amount"
    DAYS = "Bill Ageing (In Days)"
    Report = namedtuple("Report",['summary','breakup'])
    def generate_report(df,days) -> Report :  
       unfiltered =df.copy()
       filtered =df[df[DAYS] > days ]   
       pivot_filtered = pd.pivot_table(filtered,index=[SALESMAN,BEAT,PARTY],values=[OS_AMT,DAYS],aggfunc={OS_AMT:sum,DAYS:max},margins=True)
       pivot_filtered_splitup = pd.pivot_table(filtered,index=[SALESMAN,BEAT,PARTY,BILL_NO],values=[OS_AMT,DAYS],aggfunc={OS_AMT:sum,DAYS:max},margins=True)
       pivot_total_bills = pd.pivot_table(unfiltered,index=[SALESMAN,BEAT,PARTY],values=[BILL_NO],aggfunc={BILL_NO:pd.Series.nunique},margins=True)
       pivot_filtered =pd.merge(pivot_total_bills,pivot_filtered,left_index=True,right_index=True)
       pivot_filtered=pivot_filtered[[BILL_NO,DAYS,OS_AMT]]
       return Report(summary = pivot_filtered, breakup = pivot_filtered_splitup)
 
    after_30_days = generate_report( pending_bills[ pending_bills[DAYS] <= 365 ] , 29 ).breakup
    today_21_days_summary , today_21_days_breakup = generate_report( today_pending_bills , 20 )
     

    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    today_21_days_summary.to_excel(writer, sheet_name='Summary')
    today_21_days_breakup.to_excel(writer, sheet_name='Detail')
    after_30_days.to_excel(writer, sheet_name='30 Days')
    today_pending_bills.to_excel(writer, sheet_name='Today')
    writer.save()
    output.seek(0)

    response = HttpResponse(output.getvalue(), content_type='application/vnd.ms-excel')
    response['Content-Disposition'] = 'attachment; filename="' + "outstanding.xlsx" + '"'
    return response

lock = threading.Lock()

billing_process = { "SYNC" : Billing.Sync , "PREVBILLS" : Billing.Prevbills , "COLLECTION" : Billing.Collection , 
     "ORDER" : Billing.Order , "DELIVERY" : Billing.Delivery , "DOWNLOAD" : Billing.Download , 
     "PRINT" : Billing.Printbill }
    
def update_creditlock_table(creditlock_data , billing ) : 
    objs = defaultdict(lambda : None)
    for party , data in creditlock_data.items() :
        obj = models.CreditLock( party = party , beat = data["beat_name"] ,  bills = data["bills"],value = data["billvalue"],
                       salesman = data["salesman"] , collection = data["coll_str"], phone = data["ph"] , data = data , billing = billing)
        obj.save() 
        for on in data["orders"] : 
            objs[on] = obj
    return objs 

def run_billing_process(billing:Billing,billing_log) : 
    for process_name,process in billing_process.items() : 
        obj = models.ProcessStatus.objects.get(billing=billing_log,process=process_name)
        obj.status = 2
        start_time = time.time()
        obj.save()    
        try : 
            process(billing)
            if process_name == "ORDER" : 
                orders_to_creditlock = update_creditlock_table(billing.creditlock,billing_log)
                orders = billing.order_original
                
                models.Orders.objects.bulk_create([ 
                    models.Orders(order_no=row.on,party = row.p,salesman=row.s,creditlock=orders_to_creditlock[row.on], 
                        billing = billing_log , date = datetime.datetime.now().date() , type = row.ot ) 
                    for _,row in orders.drop_duplicates(subset="on").iterrows() ],
                 update_conflicts=True,
                 unique_fields=['order_no'],
                 update_fields=['creditlock_id',"billing_id","type"])
                
                models.OrderProducts.objects.bulk_create([ models.OrderProducts(order_id=row.on,product=row.bd,quantity=row.cq,allocated = row.aq,rate = row.t) 
                    for _,row in orders.iterrows() ] , 
                 update_conflicts=True,
                 unique_fields=['order_id','product'],
                 update_fields=['allocated','reason'])

            if process_name == "COLLECTION" : 
               models.PushedCollection.objects.bulk_create([ models.PushedCollection(
                   billing = billing_log, party_code = pc) for pc in billing.pushed_collection_ids ])
            if process_name == "DELIVERY" and billing.bills : 
                billing_log.start_bill_no = billing.bills[0]
                billing_log.end_bill_no = billing.bills[-1]
                billing_log.bill_count = len(billing.bills)
                billing_log.save()

            obj.status = 1
        except Exception as e :
            traceback.print_exc()
            billing_log.error = str(traceback.format_exc())
            obj.status = 3
        
        end_time = time.time()
        time_taken = end_time - start_time
        obj.time = round(time_taken,2)
        obj.save()
        if obj.status == 3 : 
            billing_log.end_time = datetime.datetime.now() 
            billing_log.status = 3 
            billing_log.save()
            lock.release()
            return 
        
    billing_log.end_time = datetime.datetime.now() 
    billing_log.status = 1 
    billing_log.save()       
    lock.release()
   
def start(request, queryset) :
    line_count = int(request.POST["line_count"])
    if not lock.acquire(blocking=False) : 
        return False
     
    creditrelease = { row.party : row.data for row in queryset }
    today = timezone.now().date()
    today_pushed_collections = models.PushedCollection.objects.filter( billing__start_time__gte = today ).values_list("party_code",flat=True)
    lines_count = { order.order_no : order.products.count() for order in models.Orders.objects.filter(date = datetime.datetime.now().date()) }
    
    print( lines_count )
    print( creditrelease , today_pushed_collections )
    
    billing_log = models.Billing(start_time = datetime.datetime.now(), status = 2)
    billing_log.save() 
    for process_name in billing_process :
        models.ProcessStatus(billing = billing_log,process = process_name,status = 0).save()    
    billing = Billing(max_lines = line_count,creditrelease = creditrelease, prev_collection= today_pushed_collections,lines_count = lines_count)
    thread = threading.Thread( target = run_billing_process , args = (billing,billing_log) )
    thread.start() 
    return True 

def generate_bill_statistics() :
    
    today = (timezone.now()).date()
    
    today_stats = models.Billing.objects.filter(start_time__gte = today).aggregate( 
        bill_count = Sum("bill_count") , collection_count = Count("collection") , 
        bills_start = Min("start_bill_no") , bills_end = Min("end_bill_no") , 
        success = Count("status",filter=Q(status = 1)) , failed = Count("status",filter=Q(status = 3))
    )

    last_stats = models.Billing.objects.filter(start_time__gte = today).last()

    if last_stats is not None : 
        stats = {"LAST BILLS COUNT" : last_stats.bill_count or 0,"LAST COLLECTION COUNT" : last_stats.collection.count() ,  
                    "LAST BILLS" : f'{last_stats.start_bill_no or ""} - {last_stats.end_bill_no or ""}'}
    else : 
        stats = {"LAST BILLS COUNT" : 0 ,"LAST COLLECTION COUNT" : 0 , "LAST BILLS" : "-" }
    
    stats |= { "TODAY BILLS COUNT" : today_stats["bill_count"] , "TODAY COLLECTION COUNT" : today_stats["collection_count"] , 
    "TODAY BILLS" : f'{today_stats["bills_start"]} - {today_stats["bills_end"]}'  ,"SUCCESSFULL" : today_stats["success"] , 
    "FAILURES" : today_stats["failed"] }
    
    models.BillStatistics.objects.all().delete()
    models.BillStatistics.objects.bulk_create([ models.BillStatistics(type = k,count = str(v)) for k,v in stats.items() ])


class ProcessStatusAdmin(admin.ModelAdmin):
    actions = None
    ordering = ("id",)
    
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        last_process = qs.last()
        if last_process is None : return qs 
        return qs.filter(billing = last_process.billing)
    
    def colored_status(self,obj):
        class_name = ["unactive","green","blink","red"][obj.status]
        return format_html(f'<span class="{class_name} indicator"></span>')
    colored_status.short_description = ""

    def time(obj):
        return (f"{obj.time} SEC") if obj.time is not None else '-'
    
    list_display = ["colored_status","process",time]
    
class LastBillStatisticsAdmin(admin.ModelAdmin):
    actions = None
    list_display = ["type","count"]
    def get_queryset(self, request) :
        return models.BillStatistics.objects.filter( type__contains = "LAST" ).reverse()
 
class TodayBillStatisticsAdmin(admin.ModelAdmin):
    actions = None
    list_display = ["type","count"]
    def get_queryset(self, request) :
        return models.BillStatistics.objects.exclude( type__contains = "LAST" ).reverse()

class ProductAdmin(admin.TabularInline) : 
    model = models.OrderProducts

class OrderAdmin(admin.ModelAdmin) : 
    inlines = [ProductAdmin]

    def bill_value(self,obj) : 
        return round(obj.bill_value) 
    
    def allocated_value(self,obj) : 
        return round(obj.allocated_value) 
    
    def get_queryset(self, request: HttpRequest) -> QuerySet[Any]:
        return super().get_queryset(request).annotate(bill_value = Sum(F("products__quantity")*F("products__rate")),
                                                      allocated_value = Sum(F("products__allocated")*F("products__rate")) )
    
    list_display = ["order_no","date","party","salesman","bill_value","allocated_value","type"]
    list_filter = ["date","salesman","party","billing"]

class OrderInline(admin.TabularInline) : 
    model = models.Orders
    show_change_link = True
    verbose_name_plural = "orders"
    
class BillingAdmin(admin.ModelAdmin) :   
    change_list_template = "billing.html"
    list_display = ["party","beat","value","bills","salesman","collection","phone"]
    inlines = [OrderInline]
    actions = [start]
    
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        last_process = qs.last()
        if last_process is None : return qs 
        return qs.filter(billing = last_process.billing)
    
    def changelist_view(self, request, extra_context=None):   
        time_interval = int( request.POST.get("time_interval",10) )
        time_interval_milliseconds = time_interval * 1000 * 60
        next_action = "unknown"
        generate_bill_statistics()

        if request.method == "POST" : 
            if request.POST.get("_selected_action") :
                selected = request.POST.getlist(helpers.ACTION_CHECKBOX_NAME)
                queryset = models.CreditLock.objects.filter(pk__in = selected)
                post = request.POST.copy()
                post.pop("_selected_action", None)
                request._set_post(post)
            else : 
                queryset = models.CreditLock.objects.none()


            action = request.POST.get("action_name")
            if action == "outstanding" : 
                return outstanding()
            
            if action == "start" : 
                if not start(request,queryset) : 
                    time_interval_milliseconds = 5 * 1000
            if action == "start" : 
                next_action = "refresh"
            if action == "refresh" :
                next_action = "refresh" if lock.locked() else "start"
            if action == "quit" : 
                time_interval_milliseconds = int(1e7)
            if next_action == "refresh" :
                time_interval_milliseconds = 5 * 1000
     
        cl1 = LastBillStatisticsAdmin(models.BillStatistics,admin_site).get_changelist_instance(request)
        cl1.formset = None
        cl2 = ProcessStatusAdmin(models.ProcessStatus,admin_site).get_changelist_instance(request)
        cl2.formset = None   
        cl3 = TodayBillStatisticsAdmin(models.BillStatistics,admin_site).get_changelist_instance(request)
        cl3.formset = None

        line_count = int(request.POST.get("line_count",100))
        return super().changelist_view( request, extra_context={ "title" : "abcd","cl1":cl1,"cl2" : cl2 , "cl3" : cl3 ,  
        "time_interval_milliseconds" : time_interval_milliseconds , "time_interval" : time_interval , 
        "line_count" : line_count , "auto_action_type" : next_action })

admin_site.register(models.CreditLock,BillingAdmin)
admin_site.register(models.Orders,OrderAdmin)

