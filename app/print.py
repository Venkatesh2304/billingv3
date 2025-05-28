import base64
import datetime
import hashlib
from io import BytesIO
from json import JSONDecodeError
import json
import os
import shutil
import threading
import time
import traceback
from PyPDF2 import PdfMerger
from django import forms
from django.http import FileResponse, HttpResponse, JsonResponse
import pandas as pd
from enum import Enum, IntEnum
from app.common import bulk_raw_insert, query_db
import app.models as models 
from app.sync import sync_reports
from custom import secondarybills
from custom.classes import Billing, Einvoice
from django.db.models import Max,F,Min,Q,F
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.db.models import CharField, Value
from django.db.models.functions import Concat,Coalesce
import app.pdf_create as pdf_create
import app.aztec as aztec 
from django.views.decorators.http import condition
import hashlib
import json
from functools import wraps
from django.http import JsonResponse, HttpResponseNotModified
import app.enums as enums
from billingv3.settings import FILES_DIR 

class PrintType(Enum):
        FIRST_COPY = "first_copy"
        DOUBLE_FIRST_COPY = "double_first_copy"
        SECOND_COPY = "second_copy"
        LOADING_SHEET = "loading_sheet"
        LOADING_SHEET_SALESMAN = "loading_sheet_salesman"

def einvoice_upload(einvoice_service,einv_qs) :
    billing = Billing()
    dates = einv_qs.aggregate(fromd=Min("bill__date"), tod=Max("bill__date"))
    from_date, to_date = dates["fromd"], dates["tod"]
    # Generate e-invoice JSON from Billing
    inums = einv_qs.values_list("bill_id", flat=True)
    bytesio = billing.einvoice_json(fromd=from_date, tod=to_date, bills=inums)
    err = ""
    if bytesio : 
        json_str = bytesio.getvalue().decode('utf-8')  # Convert BytesIO to string
        success, failures = einvoice_service.upload(json_str)
        failed_inums = failures.get("Invoice No", []).tolist()
        if failures.shape[0]:
            print(failures)
            err = f"E-Invoice upload failed for {failed_inums}"
    else :
        failed_inums = inums
        err = "No data generated for e-invoice upload."

    # Process today's e-invoices
    today_einvs_bytesio = BytesIO(einvoice_service.get_today_einvs())
    try :
        response = billing.upload_irn(today_einvs_bytesio)
    except JSONDecodeError as e : 
        err = "Error uploading irn to ikea"

    # Handle IRN upload failures
    if not response["valid"]:
        raise Exception(f"IRN Upload Failed: {response}")

    # Process the successful e-invoices
    einvoice_df = pd.read_excel(today_einvs_bytesio)
    for _,row in einvoice_df.iterrows() : 
        models.Bill.objects.filter(bill_id = row["Doc No"].strip()).update(irn = row["IRN"].strip())
            
    # Remove successfully processed invoices from the failed list
    processed_bills = einvoice_df["Doc No"].values
    failed_inums = list(set(failed_inums) - set(processed_bills))
    return ( err == ""  , err , failed_inums )



@api_view(["POST"])
def print_bills(request) : 
    data = request.data
    full_print_type = data.get("print_type")
    
    print_downloads = { 
        "both_copy" : [PrintType.FIRST_COPY, PrintType.SECOND_COPY],
        "first_copy" : [PrintType.FIRST_COPY],
        "double_first_copy" : [PrintType.FIRST_COPY],
        "second_copy" : [PrintType.SECOND_COPY],
        "loading_sheet" : [PrintType.LOADING_SHEET],
        "loading_sheet_salesman" : [PrintType.LOADING_SHEET_SALESMAN]
    }
    billing = Billing()
    bills = data.get("bills",[])
    bills.sort()
    if len(bills) == 0 :
        return JsonResponse({"status" : "error" , "error" : "Zero Bills Selected to print"})
    qs = models.Bill.objects.filter(bill_id__in = bills)
    #Remove already printed , if not loading sheet
    if full_print_type != "loading_sheet" : 
        loading_sheets = list(qs.values_list("loading_sheet",flat=True).distinct())
        qs.update(print_time=None,loading_sheet=None,is_reloaded = True)
        models.SalesmanLoadingSheet.objects.filter(inum__in = loading_sheets).delete()
        qs = qs.all() #Refetch queryset
    if full_print_type == "reload_bill" : 
        return JsonResponse({"status" : "success"})
    context = { 'salesman':  data.get("salesman") , 'beat': data.get("beat") , 
                'party' : data.get("party") , 'inum' : "SM" + bills[0] }
    
    einvoice_upload_error = ""
    einvoice_enabled = models.Settings.objects.get(key = "einvoice").status 
    if einvoice_enabled : 
        einv_qs =  qs.filter(bill__ctin__isnull=False, irn__isnull=True)
        if einv_qs.count()   : 
            einvoice_service = Einvoice() 
            if einvoice_service.is_logged_in() :
                (success,einvoice_upload_error,failed_inums) = einvoice_upload(einvoice_service,einv_qs)
            else : 
                return JsonResponse({"status" : "einvoice_login"})
            
    for print_type in print_downloads[full_print_type] :
        if print_type == PrintType.FIRST_COPY : 
            billing.Download(bills=bills,pdf=True, txt=False,cash_bills=[])
            bill_pdf_path = os.path.join(FILES_DIR, "bill.pdf")
            pdf_create.remove_blank_pages_from_first_copy(bill_pdf_path)
            aztec.add_aztec_codes_to_pdf(bill_pdf_path,bill_pdf_path,PrintType.FIRST_COPY)
            qs.update(print_type = print_type.value,print_time = datetime.datetime.now())

        elif print_type == PrintType.SECOND_COPY :
            billing.Download(bills=bills,pdf=False, txt=True,cash_bills=[])
            secondarybills.main(f'{FILES_DIR}/bill.txt', f'{FILES_DIR}/bill.docx',aztec.generate_aztec_code)
            
        elif print_type == PrintType.LOADING_SHEET :
            pdf_create.loading_sheet_pdf(billing.loading_sheet(bills),sheet_type=pdf_create.LoadingSheetType.Plain) 
            models.Bill.objects.filter(bill_id__in = bills).update(plain_loading_sheet=True)

        elif print_type == PrintType.LOADING_SHEET_SALESMAN :
            pdf_create.loading_sheet_pdf(billing.loading_sheet(bills), 
                                        sheet_type=pdf_create.LoadingSheetType.Salesman,
                                                    context=context)
            loading_pdf_path = os.path.join(FILES_DIR, "loading.pdf")
            aztec.add_aztec_codes_to_pdf(loading_pdf_path,loading_pdf_path,PrintType.LOADING_SHEET_SALESMAN)
            loading_sheet = models.SalesmanLoadingSheet.objects.create(**context)
            qs.update(print_type = print_type.value,print_time = datetime.datetime.now(),loading_sheet = loading_sheet)
        else : 
            pass 
            
    print_files = { 
        "both_copy" : ["bill.pdf","bill.docx"],
        "first_copy" : ["bill.pdf"],
        "double_first_copy" : ["bill.pdf","bill.pdf"],
        "second_copy" : ["bill.docx"],
        "loading_sheet" : ["loading.pdf"],
        "loading_sheet_salesman" : ["loading.pdf","loading.pdf"]
    }
    
    merger = PdfMerger()
    for file in print_files[full_print_type] :
        file = f"{FILES_DIR}/{file}"
        if file.endswith(".docx") : 
            os.system(f"libreoffice --headless --convert-to pdf {file} --outdir {FILES_DIR}")
            file = file.replace(".docx",".pdf") 
        with open(file, "rb") as pdf_file:
            merger.append(pdf_file)
    merger.write(f"{FILES_DIR}/bill.pdf")
    merger.close()
    
    return JsonResponse({"status" : "success" , "error" : einvoice_upload_error })

