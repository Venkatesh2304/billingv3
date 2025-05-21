import time
from django import forms
import app.models as models
import datetime
from app.admin import billing_process_names,run_billing_process,BillingStatus,billing_lock
from django.core.management.base import BaseCommand

class BillingForm(forms.Form):
    date = forms.DateField()
    max_lines = forms.IntegerField()

class Command(BaseCommand):
    help = "Command to run automatic billing"
    def run_billing(self) : 
        last_billing = models.Billing.objects.order_by("-start_time").first()
        if (last_billing.status == BillingStatus.Started) and (last_billing.start_time < datetime.datetime.now() + datetime.timedelta(minutes = 10)) :
            return False

        billing_lock.acquire()
        billing_form = BillingForm({"date":datetime.date.today(),"max_lines":100})
        billing_form.is_valid()
        billing_log = models.Billing(start_time = datetime.datetime.now(), status = 2,date = billing_form.cleaned_data.get("date"), automatic = True )
        billing_log.save()  
        for process_name in billing_process_names :
            models.BillingProcessStatus(billing = billing_log,process = process_name,status = 0).save()
        run_billing_process(billing_log,billing_form)
        return (billing_log.status == BillingStatus.Success)
    
    def handle(self, *args: models.Any, **options: models.Any) -> str | None:
        print(f"Automatic Billing Started @ {datetime.datetime.now()}")
        for i in range(3) : 
            if self.run_billing() : 
                break 
        self.run_billing()
        

