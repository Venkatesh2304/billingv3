import base64
from collections import Counter, defaultdict
from collections import abc
import random
from django.db import transaction
import datetime
from functools import partial, update_wrapper
import functools
from io import BytesIO
import json
import shutil
from PyPDF2 import PdfMerger, PdfReader, PdfWriter
import custom.secondarybills  as secondarybills
from dal import autocomplete
import logging
import multiprocessing
import os
import re
import shutil
from threading import Thread
import threading
import django
from django.contrib.admin.views.main import ChangeList
import time
import traceback
from typing import Any, Dict, Optional, Type
from django import forms
from django.contrib import admin
from django.contrib.admin.views.main import ChangeList
from django.core.handlers.wsgi import WSGIRequest
from django.db.models.base import Model
from django.db.models.query import QuerySet
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.http.request import HttpRequest
from django.shortcuts import redirect, render
from django.template.response import TemplateResponse
from django.urls.resolvers import URLPattern
import numpy as np
from openpyxl import load_workbook
import pandas as pd
from enum import Enum, IntEnum
from app.common import both_insert, bulk_raw_insert, query_db
import app.models as models 
from django.utils.html import format_html
from django.contrib.admin.templatetags.admin_list import register , result_list  
from django.contrib.admin.templatetags.base import InclusionAdminNode
from custom.Session import Logger  
from typing import Callable
from custom.classes import Billing,IkeaDownloader,Einvoice
from django.utils.safestring import mark_safe
from django.utils import timezone
from django.db.models import Max,F,Subquery,OuterRef,Q,Min,Sum,Count
from collections import namedtuple
from app.sales_import import AdjustmentInsert, BeatInsert, CollectionInsert, PartyInsert, SalesInsert
from django.urls import path, reverse, reverse_lazy
from django.contrib import messages
import dal.autocomplete
from pytz import timezone
from custom.Session import client
import app.pdf_create as pdf_create
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode
from rangefilter.filters import NumericRangeFilter
import os 
from requests.exceptions import JSONDecodeError
from django.http import JsonResponse
from urllib.parse import quote, unquote
# from django_admin_multi_selecfrom django.db.models.functions import Concat
from django.db.models import CharField, Value
from django.db.models.functions import Concat
import os 
user = os.environ.get("app_user")
START_DATE = datetime.date(2025 if "lakme" in user else 2024,4,1)

def check_last_sync(type,limit) :
    if limit is None : return False 
    last_synced = models.Sync.objects.filter( process = type.capitalize() ).first()
    if last_synced : 
        if isinstance(limit,int) :  
            if (datetime.datetime.now() - last_synced.time).seconds <= limit : return True
        elif isinstance(limit,datetime.date) : 
            if last_synced.time.date() >= limit : return True
        elif isinstance(limit,datetime.datetime):  
            if last_synced.time >= limit : return True
        else : 
            raise Exception(f"Limit specified for {type} = {limit} is not an instance of int,datetime.date or datetime.datetime")
    return False 

sync_reports_lock = threading.Lock()
def sync_reports(billing = None,limits = {},min_days_to_sync = {},retry_no=3) -> bool :

    with sync_reports_lock : 
        today = datetime.date.today()
        min_days_to_sync = defaultdict(lambda : 2 , min_days_to_sync )
        get_sync_from_date_for_model = lambda model_class : max(min( model_class.objects.aggregate(date = Max("date"))["date"] or START_DATE ,
                                                        today - datetime.timedelta(days=min_days_to_sync[model_class.__name__.lower()]) ), START_DATE)
        
        DeleteType = Enum("DeleteType","datewise all none")
        FunctionTuple = namedtuple("function_tuple",["download_function","model","insert_function","has_date_arg","delete_type"])
        function_mappings = { "sales" : FunctionTuple(Billing.sales_reg,models.Sales,SalesInsert,has_date_arg=True,delete_type=DeleteType.datewise) , 
                            "adjustment" : FunctionTuple(Billing.crnote,models.Adjustment,AdjustmentInsert,has_date_arg=True,delete_type=DeleteType.datewise) , 
                            "collection" : FunctionTuple(Billing.collection,models.Collection,CollectionInsert,has_date_arg=True,delete_type=DeleteType.datewise) , 
                            "party" : FunctionTuple(Billing.party_master,None,PartyInsert,has_date_arg=False,delete_type=DeleteType.none) , 
                            "beat" : FunctionTuple(Billing.get_plg_maps,models.Beat,BeatInsert,has_date_arg=False,delete_type=DeleteType.all) , 
                            }
        
        insert_types_to_update = []
        for insert_type,limit in limits.items() : 
            if insert_type not in function_mappings : raise Exception(f"{insert_type} is not a valid Insert type")
            if not check_last_sync(insert_type,limit) : insert_types_to_update.append(insert_type)

        if len(insert_types_to_update) == 0 : return False 
        if billing is None : billing = Billing()
        with ThreadPoolExecutor() as executor:
            futures = []
            
            for insert_type in insert_types_to_update : 
                functions = function_mappings[insert_type]
                def retry_wrapped_download_fn(*args) :
                    for retry in range(retry_no) :
                        try : 
                            df = functions.download_function(*args)
                            return df
                        except Exception as e : 
                            traceback.print_exc()
                            print(f"Error in Downloading {insert_type} : {e}")
                    raise Exception(f"Failed Downloading {insert_type} after {retry_no} retries")

                if functions.has_date_arg : 
                    last_updated_date = get_sync_from_date_for_model(functions.model)
                    futures.append( executor.submit(retry_wrapped_download_fn, billing, last_updated_date, today) )
                else : ## No date argument required 
                    futures.append( executor.submit(retry_wrapped_download_fn, billing) )

            for insert_type,future in zip(insert_types_to_update,futures) :
                functions = function_mappings[insert_type]
                try : 
                    df = future.result()
                    with transaction.atomic():
                        if functions.delete_type == DeleteType.datewise : 
                            last_updated_date = get_sync_from_date_for_model(functions.model)
                            functions.model.objects.filter(date__gte = last_updated_date).delete()
                        elif functions.delete_type == DeleteType.all : 
                            functions.model.objects.all().delete()
                        elif functions.delete_type == DeleteType.none : 
                            pass 
                        else : 
                            raise Exception(f"{functions.delete_type} is not a valid delete type")
                        functions.insert_function(df)
                        models.Sync.objects.update_or_create(process = insert_type.capitalize() , defaults={"time" : datetime.datetime.now()})
                        print(f"Syncing {insert_type} Done")
                except Exception as e : 
                    traceback.print_exc()
                    print(f"Error in Syncing {insert_type} : {e}")

        return True 
