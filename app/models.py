from collections import defaultdict
import datetime
from typing import Any, Iterable, Optional
from django.db import connection, models
from django.db.models import CharField,IntegerField,FloatField,ForeignKey,DateField
from django.db.models import Sum,F
from django.db.models import F, ExpressionWrapper, FloatField, Sum
from django.db.models import Max,F,Subquery,OuterRef,Q,Min,Sum,Count
from custom.Session import client
import pandas as pd
from pytz import timezone
from app.common import query_db
from bson import ObjectId


## Billing Models
class Billing(models.Model) : 
    start_time = models.DateTimeField()
    end_time = models.DateTimeField(null=True,blank=True)
    status = models.IntegerField()
    error = models.TextField(max_length=100000,null=True,blank=True)
    start_bill_no = models.TextField(max_length=10,null=True,blank=True)
    end_bill_no = models.TextField(max_length=10,null=True,blank=True)
    bill_count = models.IntegerField(null=True,blank=True,default=0)
    date = models.DateField()
    automatic = models.BooleanField(default=False,db_default=False)

    def __str__(self) -> str:
        print("x",self.start_time)
        return str(self.start_time.strftime("%d/%m/%y %H:%M:%S"))
     
class PushedCollection(models.Model) : 
    billing = models.ForeignKey(Billing,on_delete=models.CASCADE,related_name="collection")
    party_code = models.TextField(max_length=30)

class Orders(models.Model) : 
    
    order_no = models.TextField(max_length=60,primary_key=True)
    salesman = models.TextField(max_length=30)
    date = 	models.DateField()
    type = models.TextField(max_length=15,choices=(("SH","Shikhar"),("SE","Salesman")),blank=True,null=True)
    billing = models.ForeignKey(Billing,on_delete=models.CASCADE,related_name="orders",null=True,blank=True)
    party = models.ForeignKey("app.Party",on_delete=models.DO_NOTHING,related_name="orders")
    beat = models.ForeignKey("app.Beat",on_delete=models.DO_NOTHING,related_name="orders",db_constraint=False,db_index=False)
    place_order = models.BooleanField(default=False,db_default=False)
    force_order = models.BooleanField(default=False,db_default=False)
    creditlock = models.BooleanField(default=False,db_default=False)
    release = models.BooleanField(default=False,db_default=False)
    delete = models.BooleanField(default=False,db_default=False)
    # partial = models.BooleanField(default=False,db_default=False)

    ##Expressions 
    @property
    def bill_value(self) : 
        return round( sum([ p.quantity * p.rate for p in self.products.all() ])   , 2 )

    @property
    def allocated_value(self) : 
        return round( sum([ p.allocated * p.rate for p in self.products.all() ]) or 0  , 2 )

    @property
    def partial(self) : 
        return bool( (self.products.filter(allocated = 0).count() and self.products.filter(allocated__gt = 0).count()) )  

    @property
    def pending_value(self) : 
        return round(self.bill_value() - self.allocated_value(),2)
    
    @property
    def OS(self) :
        today = datetime.date.today() 
        bills = [  f"{-round(bill.balance)}*{(today - bill.date).days}"
                 for bill in Outstanding.objects.filter(party = self.party,beat = self.beat.name,balance__lte = -1).all() ]
        return "/ ".join(bills) or "-"
    
    @property
    def coll(self) : 
        today = datetime.date.today() 
        coll = [  f"{round(coll.amt or 0)}*{(today - coll.bill.date).days}"
                 for coll in Collection.objects.filter(party = self.party , date = today).all() ]
        return "/ ".join(coll) or "-"
    
    @property
    def phone(self) : 
        phone = self.party.phone or "-"
        return phone #hyperlink(url = "tel:+91" + phone, text = phone)

    @property
    def lines(self) : 
        return len([ product for product in self.products.all() if product.allocated != product.quantity])
    
    @property
    def partial(self) :
        return self.partial()

    @property
    def cheque(self) : 
        #warning : disabled salesman cheque collection
        # qs = SalesmanCollection.objects.filter(time__gte = datetime.date.today()).filter(bills__inum__party = self.party)
        # colls = qs.all()
        colls = []
        if len(colls) : 
            ids = ",".join([ str(coll.id) for coll in colls ])
            day_values = defaultdict(lambda : 0) 
            today = datetime.date.today()
            for coll in colls : day_values[(coll.date - today).days] += coll.amt  
            return hyperlink(f'/app/salesmancollection/?id__in={ids}',"/".join([ f"{round(amt)}*{day}" for day,amt in day_values.items() ])) 
        else : 
            return ""
    
    class Meta : 
        verbose_name = 'Orders'
        verbose_name_plural = 'Billing'

class OrdersProxy(Orders):
    class Meta:
        proxy = True
        verbose_name = "order"

class OrderProducts(models.Model) : 
    order = models.ForeignKey(Orders,on_delete=models.CASCADE,related_name="products")
    product = models.TextField(max_length=100)
    batch = models.TextField(max_length=10,default="00000",db_default="00000")
    quantity =  models.IntegerField()
    allocated =  models.IntegerField()
    rate = models.FloatField()
    reason = models.TextField(max_length=50)
    # billed = models.BooleanField(default=False,db_default=False)
    
    def __str__(self) -> str:
         return self.product
    
    class Meta:
        unique_together = ('order', 'product','batch')

class BillStatistics(models.Model) : 
    type = models.TextField(max_length=30)	
    count = models.TextField(max_length=30) 

class BillingProcessStatus(models.Model) : 
    billing = models.ForeignKey(Billing,on_delete=models.CASCADE,related_name="process_status",null=True,db_constraint=False)
    status = models.IntegerField(default=0)
    process = models.TextField(max_length=30)	
    time = models.FloatField(null=True,blank=True) 

class AbstractProcessStatus(models.Model) : 
    status = models.IntegerField(default=0)
    process = models.TextField(max_length=30,unique=True)	
    time = models.FloatField(null=True,blank=True) 
    class Meta :
         abstract = True 

class BasepackProcessStatus(AbstractProcessStatus) : pass



## Models For Accounting
## Abstract models

class PartyVoucher(models.Model) : 
      inum = CharField(max_length=20,primary_key=True)
      party = ForeignKey("app.Party",on_delete=models.DO_NOTHING,null=True)
      date = DateField()
      amt = FloatField(null=True)
      columns = ["inum","party_id","date","amt"]

      def __str__(self) -> str:
            return self.inum

      class Meta : 
            abstract = True 

class GstVoucher(models.Model) : 
      ctin = CharField(max_length=20,null=True,blank=True)
      gst_period = CharField(max_length=12,null=True,blank=True)
      
      @property
      def txval(self) : 
          return abs( round( self.invs.aggregate(s = Sum(F("txval")))["s"],3) )
      
      @property
      def tax(self) : 
          return abs( round( self.invs.aggregate(s = Sum(F("txval") * F("rt") / 100 ))["s"],3) )

      class Meta : 
            abstract = True 

class Party(models.Model) : 
      code = CharField(max_length=10,primary_key=True)
      master_code = CharField(max_length=10,null=True,blank=True)
      name = CharField(max_length=50,null=True,blank=True)
      type = CharField(db_default="shop",max_length=10,null=True)
      addr = CharField(max_length=150,blank=True,null=True)
      pincode = IntegerField(blank=True,null=True)
      ctin = CharField(max_length=20,null=True,blank=True)
      phone = CharField(max_length=20,null=True,blank=True)
      hul_code = CharField(max_length=40,null=True,blank=True)

      def __str__(self) -> str:
            return self.name or self.code 
      @property
      def identifier(self) : 
           return f"{self.name} ({self.name})" 
     
      class Meta : 
            verbose_name_plural = 'Party'

class Sales( PartyVoucher,GstVoucher ) :
      discount = FloatField(default=0,db_default=0)
      roundoff = FloatField(default=0,db_default=0)
      type = CharField(max_length=15,db_default="sales",null=True)
      tds = FloatField(default=0,db_default=0)
      tcs = FloatField(default=0,db_default=0)
      columns = PartyVoucher.columns + ["ctin","roundoff","type","discount","beat"]
      beat = models.TextField(max_length=40,null=True)
      delivered = models.BooleanField(default=True,db_default=True)
      class Meta :
        verbose_name_plural = 'Sales'


class Collection( PartyVoucher ) : 
      
      bill = ForeignKey("app.Sales",db_index=False,db_constraint=False,on_delete=models.DO_NOTHING)
      mode = CharField(max_length=30)
      bank_entry = ForeignKey("app.BankStatement",related_name="ikea_collection",db_index=False,db_constraint=False,on_delete=models.DO_NOTHING,null=True,blank=True)
      columns = ["inum","date","amt"] + ["bill_id","mode","party_id","bank_entry_id"]

      @property
      def Mode(self) : return (self.mode or "").upper()
      
      class Meta : 
            verbose_name_plural = 'Collection'

class Adjustment( PartyVoucher ) : 
    #   inum = CharField(max_length=20)
      ##from_bill can be null in case of excess collection as the reason
      from_bill = ForeignKey("app.Sales",null=True,db_index=False,db_constraint=False,on_delete=models.DO_NOTHING,related_name="adjusted_from") 
      to_bill = ForeignKey("app.Sales",db_index=False,db_constraint=False,on_delete=models.DO_NOTHING,related_name="adjusted_to")
      adj_amt = FloatField(default=0)
      columns = PartyVoucher.columns + ["from_bill_id","to_bill_id","adj_amt"]
      class Meta : 
            unique_together = ("inum","from_bill","to_bill")
            verbose_name_plural = 'Adjustment'

class OpeningBalance(models.Model) : 
      party = ForeignKey("app.Party",on_delete=models.CASCADE)
      inum = CharField(max_length=20,primary_key=True)
      amt = FloatField(blank=True,null=True)
      beat = models.TextField(max_length=40)

class Beat(models.Model) : 
     id = IntegerField(primary_key=True)
     name = models.TextField(max_length=40)
     salesman_id = IntegerField()
     salesman_code = CharField(max_length=30)
     salesman_name = models.TextField(max_length=40)
     days = models.TextField(max_length=40)
     plg = models.TextField(max_length=15)
     def __str__(self) -> str:
          return self.name 
 

class SalesmanLoadingSheet(models.Model) : 
     inum = models.CharField(max_length=30,primary_key=True)
     salesman = models.TextField(max_length=30)
     party = models.TextField(max_length=30,null=True,blank=True)
     beat = models.TextField(max_length=30)
     time = models.DateTimeField(auto_now_add=True)
     
     @property
     def date(self) :
          return self.time.date() 


class Vehicle(models.Model) : 
     name = models.CharField(max_length=30,primary_key=True)
     vehicle_no = models.CharField(max_length=30)
     name_on_impact = models.CharField(max_length=30,null=True)

     def __str__(self):
          return self.name 


class Bill(models.Model) : 
    bill = models.OneToOneField("app.Sales",db_index=False,db_constraint=False,on_delete=models.DO_NOTHING,primary_key=True)
    print_time = models.DateTimeField(null=True,blank=True)
    print_type = models.TextField(max_length=20,choices=(("first_copy","First Copy"),("loading_sheet","Loading Sheet")),null=True,blank=True)
    is_reloaded = models.BooleanField(default=False,db_default=False)
    reason = models.TextField(max_length=100,null=True,blank=True)
    loading_sheet = models.ForeignKey(SalesmanLoadingSheet,on_delete=models.DO_NOTHING,related_name="bills",null=True,blank=True)
    vehicle = models.ForeignKey("app.Vehicle",on_delete=models.DO_NOTHING,related_name="bills",null=True,blank=True)
    loading_time = models.DateTimeField(null=True,blank=True)
    delivered_time = models.DateTimeField(null=True,blank=True)
    irn = models.TextField(null=True,blank=True)
    delivered = models.BooleanField(null=True,blank=True)
    delivery_reason = models.TextField(choices=(("scanned","Scanned"),
                                                ("bill_with_shop","Bill With Shop"),
                                                ("cash_bill_success","Cash Bill (Collected Money)"),
                                                ("bill_return","Bill Return"),
                                                ("qrcode_not_found","QR Code Not Found"),
                                                ("others","Other Reason")),null=True,blank=True)
    plain_loading_sheet = models.BooleanField(db_default=False,default=False)
    cash_bill = models.BooleanField(default=False,db_default=False)

    @property
    def salesman(self) :
        beat = Beat.objects.filter(name = self.bill.beat).first()
        return beat.salesman_name if beat else None
        
class Outstanding(models.Model) : 
      party = ForeignKey("app.Party",on_delete=models.CASCADE,null=True)
      inum = CharField(max_length=20,primary_key=True)
      balance = FloatField()
      beat = models.TextField(max_length=40,null=True)
      date = DateField()

      def __str__(self) -> str:
           return self.inum #+ "-" + self.party.name
                         
      @classmethod
      def upload_today_outstanding_mongo(cls) : 
          db =  client["test"]["outstandings"] ##attributes          
          print("Start Uploading today outstanding to mongo DB")
          date = str(datetime.date.today()) 
          day = datetime.datetime.strptime(date,"%Y-%m-%d").strftime("%A").lower()
          outstanding:pd.DataFrame = query_db(f"""select salesman_name as user , (select name from app_party where party_id = code) as party , inum as bill_no , -balance as amount  
              from app_outstanding left outer join app_beat on app_outstanding.beat = app_beat.name
              where  balance <= -1 and days like '%{day}%' """,is_select = True) # type: ignore
          db.delete_many({})
          if len(outstanding.index) :  db.insert_many(outstanding.to_dict(orient='records')) # type: ignore
          print("Uploaded today outstanding to mongo DB")
  
      class Meta : 
            # managed =  False
            verbose_name_plural = 'Outstanding'

class BarcodeMap(models.Model) :
     barcode = models.CharField(max_length=300,primary_key=True)
     varient = models.CharField(max_length=20,null=True,blank=True)
     sku = models.CharField(max_length=20,null=True,blank=True)

     

# class SalesmanPendingSheetX(Beat) :
#       class Meta:
#         proxy = True
     
# class RetailPrint(Bill):
#     class Meta:
#         proxy = True
#         verbose_name = "Retail Print"

# class WholeSalePrint(Bill):
#     class Meta:
#         proxy = True
#         verbose_name = "WholeSale Print"

# class BillDelivery(Bill):
#     class Meta:
#         proxy = True
#         verbose_name = "Bill Delivery"



## GST & Einvoice Related Models

# class Einvoice(models.Model) : 
#       bill = models.OneToOneField("app.Sales",on_delete=models.DO_NOTHING,related_name="einvoice",db_constraint=False,primary_key=True)
#       irn = models.TextField()
#       qrcode = models.TextField()
#       date = models.DateField()
#       json = models.TextField(null=True)


# class Eway(models.Model) : 
#       bill = models.OneToOneField("app.Sales",on_delete=models.DO_NOTHING,related_name="eway",db_constraint=False,primary_key=True)
#       ewb_no = models.TextField(null=True)
#       time = models.DateTimeField(null=True)
#     #   vehicle = ForeignKey("app.Vehicle",db_index=False,db_constraint=False,on_delete=models.DO_NOTHING,null=True,related_name="bills")

# class GstChanges(models.Model) : 
#       inum = ForeignKey("app.Sales",on_delete=models.CASCADE,db_constraint=False,db_column="inum")
#       old_ctin = CharField(null=True,blank=True,max_length=20)
#       new_ctin = CharField(null=True,blank=True,max_length=20)
#       remarks = CharField(null=True,blank=True,max_length=40)
#       class Meta:
#         verbose_name_plural = 'GST-Changes'


## Collection Models

class ChequeDeposit(models.Model) :
    BANK_CHOICES = ["KVB 650","SBI","CANARA","BARODA","UNION BANK","AXIS","HDFC","CENTRAL BANK","INDIAN BANK","IOB","ICICI","CUB","KOTAK","SYNDICATE","TMB","UNITED BANK","TCB","PGB"]
    party = ForeignKey("app.Party",on_delete=models.DO_NOTHING,null=True)
    bank = models.CharField(max_length=100, choices=zip(BANK_CHOICES,BANK_CHOICES))
    cheque_no = models.CharField(max_length=20)
    amt = models.FloatField()
    cheque_date = models.DateField()
    deposit_date = models.DateField(null=True,blank=True)
    entry_date = models.DateField(auto_now_add=True)
    def __str__(self) -> str:
         return f"CHQ: {self.cheque_no} - AMT: {self.amt} - {self.party.name}"
    
    # @property
    # def pushed(self) :
    #      return (self.bank_entry  and self.bank_entry.pushed)
    #     #  return (not self.collection.filter(pushed = False).exists())
    
class BankCollection(models.Model) : 
      bill = ForeignKey("app.Outstanding",db_index=False,db_constraint=False,on_delete=models.DO_NOTHING)
      cheque_entry = ForeignKey("app.ChequeDeposit",related_name="collection",db_index=False,db_constraint=False,on_delete=models.CASCADE,null=True,blank=True)
      bank_entry = ForeignKey("app.BankStatement",related_name="collection",db_index=False,db_constraint=False,on_delete=models.CASCADE,null=True,blank=True)
      amt = models.IntegerField()
      pushed = models.BooleanField(db_default=False,default=False)
      class Meta:
          unique_together = ('bill', 'cheque_entry', 'bank_entry')

class BankStatement(models.Model) : 
    BANK_CHOICES = ["KVB CA","SBI CA","SBI OD","SBI LAKME"]
    date = models.DateField()
    idx = models.IntegerField()
    id = models.CharField(max_length=15,primary_key=True)
    ref = models.TextField(max_length=200)
    desc = models.TextField(max_length=200)
    amt = models.IntegerField()
    bank = models.TextField(max_length=20,choices=zip(BANK_CHOICES,BANK_CHOICES))
    type = models.TextField(max_length=15,choices=(("cheque","Cheque"),("neft","NEFT"),("upi","UPI (IKEA)"),("cash_deposit","Cash Deposit"),("self_transfer","Self Transfer"),("others","Others")),null=True)
    cheque_entry = models.OneToOneField("app.ChequeDeposit", on_delete=models.DO_NOTHING, null=True, blank=True, related_name='bank_entry')
    cheque_status = models.TextField(choices=(("passed","Passed"),("bounced","Bounced")),default="passed",db_default="passed",null=True,blank=True)
    class Meta : 
        unique_together = ('date','idx','bank')
        verbose_name_plural = 'Bank'

    @property
    def status(self) : 
        if self.type is None : return 0 
        if self.type in ["cheque","neft"] :
            if self.cheque_status and (self.cheque_status == "bounced") : 
                return 3 
            all_collection_count = self.all_collection.count()
            ikea_collection_count = self.ikea_collection.count()
            is_some_not_pushed = (all_collection_count > ikea_collection_count) #self.all_collection.filter(pushed = False).exists()
            is_some_pushed = (ikea_collection_count > 0) #self.all_collection.filter(pushed = True).exists()
            if is_some_pushed : 
                return  2 if is_some_not_pushed else 3
            else : 
                return 1 
        else : 
             return 3

    @property
    def pushed(self) :
         return self.ikea_collection.exists() #.all_collection.filter(pushed = True).exists()  

    @property
    def all_collection(self) :
        return BankCollection.objects.filter(Q(bank_entry_id = self.id) | Q(cheque_entry__bank_entry = self.id))

     
class PendingSheet(models.Model) : 
    sheet_no = models.CharField(max_length=20,primary_key=True)
    date = models.DateField()
    salesman = models.TextField(max_length=30)
    beat = models.TextField(max_length=30)

class PendingSheetBill(models.Model) : 
    sheet = ForeignKey("app.PendingSheet",on_delete=models.CASCADE,related_name="bills")
    bill = ForeignKey("app.Sales",db_index=False,db_constraint=False,on_delete=models.DO_NOTHING)
    days = models.IntegerField()
    # salesman = models.TextField(max_length=30)
    # bill_amt = models.IntegerField()
    outstanding_amt = models.IntegerField()
    outstanding_on_ikea = models.IntegerField(null=True)
    outstanding_on_bill = models.IntegerField(null=True)
    outstanding_on_sheet = models.IntegerField(null=True)
    payment_mode = models.TextField(max_length=15,choices=(("cash","Cash"),("cheque","Cheque"),("neft","NEFT")),null=True)
    bill_status = models.TextField(max_length=25,choices=(("scanned","Scanned"),
                                                          ("qrcode_not_found","qrcode_not_found"),
                                                          ("loading_sheet","loading_sheet"),
                                                          ("sales_return","sales_return"),
                                                          ("bill_with_shop","Bill With Shop"),
                                                          ("others","Other Reason"),
                                                          ),null=True,default="scanned")
    class Meta : 
          unique_together = ('sheet', 'bill')

    def status(self) : 
        return bool(self.outstanding_on_bill is not None)



class TruckLoad(models.Model) :
     date = models.DateField(auto_now_add=True)
     completed = models.BooleanField(default=False,db_default=False)

class TruckPurchase(models.Model) : 
    inum = models.CharField(max_length=30,primary_key=True)
    load = models.ForeignKey("app.TruckLoad", on_delete=models.DO_NOTHING, related_name="purchases",null=True)
    
class PurchaseProduct(models.Model) :
    inum = models.ForeignKey("app.TruckPurchase", on_delete=models.CASCADE, related_name="products")
    cbu = models.CharField(max_length=20)
    sku = models.CharField(max_length=20)
    mrp = models.IntegerField()
    qty = models.IntegerField()

class TruckProduct(models.Model) :
    #  barcode = models.CharField(max_length=300,primary_key=True)
     cbu = models.CharField(max_length=20)
     qty = models.IntegerField()
     load = models.ForeignKey("app.TruckLoad", on_delete=models.DO_NOTHING, related_name="truck_products")
     box = models.IntegerField(default=1,db_default=1)  
     mrp = models.IntegerField()


# class SalesmanCollectionBill(models.Model) : 
#     chq = ForeignKey("app.SalesmanCollection",on_delete=models.CASCADE,related_name="bills")
#     inum = ForeignKey("app.Sales",db_index=False,db_constraint=False,on_delete=models.DO_NOTHING)
#     amt = models.IntegerField()
    
# class SalesmanCollection(models.Model) : 
#     # id = models.CharField(max_length=25,primary_key=True)
#     date = models.DateField()
#     amt = models.IntegerField()
#     party = ForeignKey("app.Party",on_delete=models.DO_NOTHING,null=True)
#     type = models.TextField(max_length=15,choices=(("cheque","Cheque"),("neft","NEFT")),null=True)
#     salesman = models.CharField(max_length=25,null=True,blank=True)
#     time = models.DateTimeField(null=True,auto_now_add=True)
    
# class TodayBillOut(Vehicle):
#     class Meta:
#         proxy = True
#         verbose_name = "Today Bill Out"

# class TodayBillIn(Vehicle):
#     class Meta:
#         proxy = True
#         verbose_name = "Today Bill In"

    

class Settings(models.Model):
    key = models.CharField(primary_key=True,max_length=100)  # Define keys for settings, e.g., "notifications"
    status = models.BooleanField(default=True)  
    value = models.CharField(null=True,blank=True,max_length=30)  # Store all types of values as text

class Sync(models.Model):
    process = models.CharField(max_length=20,primary_key=True)
    time = models.DateTimeField()


# from django.contrib.auth.models import AbstractUser

# class CustomUser(AbstractUser):
#     pass