from django.db import models

class Billing(models.Model) : 
    start_time = models.DateTimeField()
    end_time = models.DateTimeField(null=True,blank=True)
    status = models.IntegerField()
    error = models.TextField(max_length=100000,null=True,blank=True)
    start_bill_no = models.TextField(max_length=10,null=True,blank=True)
    end_bill_no = models.TextField(max_length=10,null=True,blank=True)
    bill_count = models.IntegerField(null=True,blank=True)
    
class CreditLock(models.Model) : 
    billing = models.ForeignKey(Billing,on_delete=models.CASCADE,null=True,blank=True,related_name="creditlocks")
    party = models.TextField(max_length=50)
    beat = 	models.TextField(max_length=50,null=True,blank=True)
    bills = models.TextField(max_length=30)	
    value = models.FloatField() 
    salesman = models.TextField(max_length=30)	
    collection = models.TextField(max_length=30)
    phone = models.TextField(max_length=30)
    data = models.JSONField()
    class Meta : 
        verbose_name = 'Billing'
        verbose_name_plural = 'Billing'

class PushedCollection(models.Model) : 
    billing = models.ForeignKey(Billing,on_delete=models.CASCADE,related_name="collection")
    party_code = models.TextField(max_length=30)

class Orders(models.Model) : 
    order_no = models.TextField(max_length=60,primary_key=True)
    billing = models.ForeignKey(Billing,on_delete=models.CASCADE,related_name="orders",null=True,blank=True)
    party = models.TextField(max_length=50)
    salesman = models.TextField(max_length=30)
    creditlock = models.ForeignKey(CreditLock,on_delete=models.SET_NULL,null=True,blank=True,related_name="orders")
    date = 	models.DateField()
    type = models.TextField(max_length=15,choices=(("SH","Shikhar"),("SE","Salesman")),blank=True,null=True)
    class Meta : 
        verbose_name = 'Orders'
        verbose_name_plural = 'Orders'

class OrderProducts(models.Model) : 
    order = models.ForeignKey(Orders,on_delete=models.CASCADE,related_name="products")
    product = models.TextField(max_length=100)
    quantity =  models.IntegerField()
    allocated =  models.IntegerField()
    rate = models.FloatField()
    reason = models.TextField(max_length=50)
    class Meta:
        unique_together = ('order', 'product')

class BillStatistics(models.Model) : 
    type = models.TextField(max_length=30)	
    count = models.TextField(max_length=30) 

class ProcessStatus(models.Model) : 
    billing = models.ForeignKey(Billing,on_delete=models.CASCADE,related_name="process_status")
    status = models.IntegerField(default=0)
    process = models.TextField(max_length=30)	
    time = models.FloatField(null=True,blank=True) 
    