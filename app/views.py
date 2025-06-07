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
from billingv3.settings import FILES_DIR
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

def etag_json_response(view_func):
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        data = view_func(request, *args, **kwargs)

        if isinstance(data, JsonResponse):
            json_data = json.loads(data.content)
        else:
            json_data = data

        content_str = json.dumps(json_data, sort_keys=True, separators=(",", ":"))
        etag = hashlib.md5(content_str.encode()).hexdigest()

        if_none_match = request.META.get("HTTP_IF_NONE_MATCH")
        if if_none_match == etag:
            return HttpResponseNotModified()

        response = JsonResponse(json_data, safe=False)
        response["ETag"] = etag
        return response

    return _wrapped_view

@etag_json_response
@api_view(["GET"])
def salesman_names(request) :
    return JsonResponse(list(models.Beat.objects.values_list("salesman_name",flat=True).distinct()),safe=False)

@etag_json_response
@api_view(["GET"])
def party_names(request) :
    qs = models.Sales.objects.filter(date__gte = datetime.date.today() - datetime.timedelta(weeks=16))
    beat = request.query_params.get('beat')
    if beat : qs = qs.filter(beat = beat)
    parties = qs.annotate(
        label = Concat( Coalesce(F('party__name'),Value('')) , Value(' ('), 'party__code', Value(')') , output_field=CharField()) , 
        value = F("party__code") , 
    ).values("label","value").distinct() #warning
    return JsonResponse(list(parties),safe=False)

@api_view(["GET"])
def download_file(request,fname) :
    response = FileResponse(open(os.path.join(FILES_DIR,fname), 'rb')) #, content_type='application/pdf')    
    response['Content-Disposition'] = f'inline; filename="{fname}"' 
    return response

@api_view(["GET","POST"])
def einvoice_login(request) : 
    einvoice_service = Einvoice()
    if request.method == "GET" :
       captcha_img = einvoice_service.captcha()
       img_base64 = base64.b64encode(captcha_img).decode('utf-8')
       return JsonResponse({"captcha" : f"data:image/png;base64,{img_base64}"})
    
    elif request.method == "POST" :
        captcha = request.data.get("captcha").strip().upper()
        is_success , error = einvoice_service.login(captcha)
        return JsonResponse({"status" : "success" if is_success else "error", "error" : error})

@api_view(["GET","POST"])
def einvoice_status(request) : 
    einv,_ = models.Settings.objects.get_or_create(key="einvoice", defaults={"status": True})
    if request.method == "POST":
        einv.status = request.data.get("enabled")
    einv.save()
    return JsonResponse({"enabled": einv.status})
