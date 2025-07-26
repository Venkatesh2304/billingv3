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
from custom.classes import Billing, Einvoice, IkeaDownloader
from django.db.models import Max,F,Min,Q,F
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.db.models import CharField, Value,Count
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
from itertools import combinations

@api_view(["GET"]) 
def cheque_match(request,bank_id) : 
    bank_entry = models.BankStatement.objects.get(id = bank_id)
    allowed_diff = 10
    qs = models.ChequeDeposit.objects.filter(deposit_date__isnull=False).filter(
                    amt__gte=bank_entry.amt - allowed_diff,
                    amt__lte=bank_entry.amt + allowed_diff
                ).filter( Q(bank_entry__isnull=True) | Q(bank_entry = bank_entry) )
    chqs = [ { "label" : str(chq) , "value" : chq.id } for chq in qs.all() ]
    return JsonResponse(chqs,safe=False)

@api_view(["GET"]) 
def bank_collection(request,bank_id) : 
    colls = models.BankStatement.objects.get(id = bank_id).all_collection
    return JsonResponse(list(colls.values("bill","amt","pushed")),safe=False)

@api_view(["POST"])
def generate_deposit_slip(request) :
    data = request.data
    queryset = models.ChequeDeposit.objects.filter(id__in = data.get("ids"))
    data = [
            {'S.NO': idx + 1, 'NAME': cheque.party.name, 'BANK': cheque.bank, 'CHEQUE NO': cheque.cheque_no, 'AMOUNT': cheque.amt , 
             'BILLS' : ','.join( cheque.collection.all().values_list("bill__inum",flat=True) ) }
            for idx, cheque in enumerate(queryset)
        ]

    # Create a new Excel file in memory
    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename=deposit_slip.xlsx'
    with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
        df = pd.DataFrame(data)
        workbook = writer.book
        worksheet = workbook.add_worksheet('DEPOSIT SLIP')
        header_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'font_size': 14,
            'border': 1
        })
        worksheet.merge_range('A1:E1', 'DEPOSIT SLIP', header_format)
        worksheet.merge_range('A2:E2', 'DEVAKI ENTERPRISES', header_format)
        worksheet.merge_range('A3:E3', 'A/C NO: 1889223000000030', header_format)
        worksheet.merge_range('A4:E4', 'PAN NO: AAPFD1365C', header_format)
        worksheet.merge_range('A5:E5', f'DATE: {datetime.date.today().strftime("%d %b %Y")}', header_format)
        df_start_row = 5
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(df_start_row, col_num, value,header_format)
        for row_num, row_data in enumerate(df.values):
            for col_num, cell_value in enumerate(row_data):
                worksheet.write(df_start_row + row_num + 1, col_num, cell_value)
        writer.sheets['DEPOSIT SLIP'] = worksheet

    queryset.update(deposit_date=datetime.date.today()) 
    return response

@api_view(["POST"])
def bank_statement_upload(request) : 
    excel_file = request.FILES['excel_file']
    bank_name = "sbi" if excel_file.name.endswith("xls") else "kvb" 
    df = pd.DataFrame()
    def skiprows_excel(excel_file,col_name,col_number,sep) : 
        df = pd.read_csv(excel_file , skiprows=0 , sep=sep , names = list(range(0,100)) , header = None)
        skiprows = -1 
        acc_no = None 
        for i in range(0,20) :   
            if df.iloc[i][col_number] == col_name : 
                skiprows = i 
                break
            x = df.iloc[i][0]
            if (type(x) == str) and ("account number" in x.lower()) :
                acc_no = df.iloc[i][1]
        df.columns = df.iloc[skiprows]
        df = df.iloc[skiprows+1:]
        return df,acc_no 
    
    ACC_BANKS = {"_00000042540766421":"SBI OD",
                 '="1889135000001946"':"KVB CA",
                 "_00000044030674591" : "SBI LAKME",
                 "_00000042536033659":"SBI CA",
                 }
    acc = None 
    if bank_name == "sbi" : 
        df,acc = skiprows_excel(excel_file,"Txn Date",col_number=0,sep = "\t")
        df = df.rename(columns={"Txn Date":"date","Credit":"amt","Ref No./Cheque No.":"ref","Description":"desc"})
        df = df.iloc[:-1]
        df["date"] = pd.to_datetime(df["date"],format='%d %b %Y')
    if bank_name == "kvb" : 
        df,acc = skiprows_excel(excel_file,"Transaction Date",col_number=0,sep = ",")
        df = df.rename(columns={"Transaction Date":"date","Credit":"amt","Cheque No.":"ref","Description":"desc"})
        df["date"] = pd.to_datetime(df["date"],format='%d-%m-%Y %H:%M:%S')
        df = df.sort_values("date")
        df["ref"] = df["ref"].astype(str).str.split(".").str[0]
    if acc and (acc in ACC_BANKS) :
        bank_name = ACC_BANKS[acc]
    else : 
        raise Exception(f"Bank acc no : {acc} doesn't have a bank")
    df["idx"] = df.groupby(df["date"].dt.date).cumcount() + 1 
    df['"desc"'] = df["desc"].copy()
    df = df[["date","ref",'"desc"',"amt","idx"]]
    df["bank"] = bank_name 
    already_assigned_ids = models.BankStatement.objects.values_list("id",flat=True).distinct()
    free_ids = list(set(range(100000,999999)) - set([int(i) for i in already_assigned_ids]))
    df["id"] = pd.Series(free_ids[:len(df.index)],index=df.index)
    df["date"] = df["date"].dt.date
    df = df[df.amt != ""][df.amt.notna()]
    df.amt = df.amt.astype(str).str.replace(",","").apply(lambda x  : float(x.strip()) if x.strip() else 0)
    df.amt = df.amt.round()
    df = df[df.amt != 0]    
    bulk_raw_insert("bankstatement",df,ignore=True,index=["idx","bank","date"])
    return JsonResponse({"status" : "success"})

@api_view(["POST"])
def push_collection(request) :
    data = request.data
    bank_entry_ids = list(data.get("ids"))
    bank_entry_ids = [ obj.id for obj in models.BankStatement.objects.filter(id__in = bank_entry_ids).exclude(cheque_status = "bounced") if not obj.pushed ]

    # unpushed_bank_ids = bank_entries.annotate(pushed_bills_count=Count('ikea_collection')).filter(pushed_bills_count=0).values_list("id",flat=True)
    # queryset = models.BankCollection.objects.filter(bank_entry__in = unpushed_bank_ids)

    cheque_entry_ids = models.BankStatement.objects.filter(id__in = bank_entry_ids).values_list("cheque_entry_id",flat=True)
    queryset = models.BankCollection.objects.filter(Q(bank_entry_id__in = bank_entry_ids) | Q(cheque_entry_id__in = cheque_entry_ids))
    # queryset = queryset.filter(pushed = False)

    billing = Billing()
    coll:pd.DataFrame = billing.download_manual_collection() # type: ignore
    manual_coll = []
    bill_chq_pairs = []
    for coll_obj in queryset.all() : 
        bank_obj = coll_obj.bank_entry or coll_obj.cheque_entry.bank_entry
        bill_no  = coll_obj.bill_id 
        row = coll[coll["Bill No"] == bill_no].copy()
        row["Mode"] = "Cheque/DD" 
        row["Retailer Bank Name"] =  coll_obj.cheque_entry.bank.upper() if coll_obj.cheque_entry else "KVB 650" 	
        row["Chq/DD Date"]  = bank_obj.date.strftime("%d/%m/%Y")
        chq_no = bank_obj.id 
        row["Chq/DD No"] = chq_no
        row["Amount"] = coll_obj.amt
        manual_coll.append(row)
        bill_chq_pairs.append((chq_no,bill_no))
    manual_coll = pd.concat(manual_coll)
    manual_coll["Collection Date"] = datetime.date.today()
    f = BytesIO()
    manual_coll.to_excel(f,index=False)
    f.seek(0)
    res = billing.upload_manual_collection(f)
    cheque_upload_status = pd.read_excel(billing.download_file(res["ul"]))
    cheque_upload_status.to_excel(f"{FILES_DIR}/cheque_upload_status.xlsx")
    sucessfull_coll = cheque_upload_status[cheque_upload_status["Status"] == "Success"]
    settle_coll:pd.DataFrame = billing.download_settle_cheque() # type: ignore
    settle_coll.to_excel(f"{FILES_DIR}/settle_cheque.xlsx")
    if "CHEQUE NO" not in settle_coll.columns : 
        return JsonResponse({"status" : "error", "file" :"cheque_upload_status.xlsx"})

    settle_coll = settle_coll[ settle_coll.apply(lambda row : (str(row["CHEQUE NO"]),row["BILL NO"]) in bill_chq_pairs ,axis=1) ]
    settle_coll["STATUS"] = "SETTLED"
    f = BytesIO()
    settle_coll.to_excel(f,index=False)
    f.seek(0)
    res = billing.upload_settle_cheque(f)
    bytes_io = billing.download_file(res["ul"])
    cheque_settlement = pd.read_excel(bytes_io)
    cheque_settlement.to_excel(f"{FILES_DIR}/cheque_settlement.xlsx")
    for _,row in sucessfull_coll.iterrows() : 
        chq_no = row["Chq/DD No"]
        bill_no = row["BillNumber"]
        models.BankStatement.objects.get(id = chq_no).all_collection.filter(bill_id = bill_no).update(pushed = True)
    sync_reports(limits = {"collection" : None})
    with pd.ExcelWriter(open(f"{FILES_DIR}/push_cheque_ikea.xlsx","wb+"), engine='xlsxwriter') as writer:
        cheque_upload_status.to_excel(writer,sheet_name="Manual Collection")
        cheque_settlement.to_excel(writer,sheet_name="Cheque Settlement")
    return JsonResponse({"status" : "success", "file" :"push_cheque_ikea.xlsx"})

@api_view(["GET"])
def unpush_collection(request,bank_id) : 
    billing = Billing()
    qs = models.BankStatement.objects.get(id = bank_id).all_collection
    if qs.count() : 
        bill_chq_pairs = [ (bank_id,bank_coll.bill_id) for bank_coll in qs.all() ]
        dates = models.BankStatement.objects.filter(id = bank_id).aggregate(
                                fromd = Min("ikea_collection__date"), tod = Max("ikea_collection__date"))
        if dates["fromd"] is not None : 
            settle_coll:pd.DataFrame = billing.download_settle_cheque("ALL",dates["fromd"],dates["tod"]) # type: ignore
            settle_coll = settle_coll[ settle_coll.apply(lambda row : (str(row["CHEQUE NO"]),row["BILL NO"]) in bill_chq_pairs ,axis=1) ]
            settle_coll["STATUS"] = "BOUNCED"
            f = BytesIO()
            settle_coll.to_excel(f,index=False)
            f.seek(0)
            res = billing.upload_settle_cheque(f)
        qs.update(pushed = False)
        sync_reports(limits = {"collection" : None})
    return JsonResponse({"status" : "success"})

@api_view(["POST"])
def auto_match_upi(request) :
    qs = models.BankStatement.objects.filter(date__gte = datetime.date.today() - datetime.timedelta(days=15)) 
    qs.filter(Q(desc__icontains="cash") & Q(desc__icontains="deposit")).update(type="cash_deposit")
    qs = qs.filter(Q(type__isnull=True)|Q(type="upi"))
    fromd = qs.aggregate(Min("date"))["date__min"]
    tod = qs.aggregate(Max("date"))["date__max"]
    upi_statement:pd.DataFrame = IkeaDownloader().upi_statement(fromd - datetime.timedelta(days = 3),tod)
    upi_statement["FOUND"] = "No"
    upi_statement["PAYMENT ID"] = upi_statement["PAYMENT ID"].astype(str).str.split(".").str[0]
    for bank_obj in qs.all() : 
        for _,row in upi_statement.iterrows() : 
            if (row["FOUND"] == "No") and (row["PAYMENT ID"] in bank_obj.desc) : 
                bank_obj.type = "upi"
                bank_obj.save()
                upi_statement.loc[_,"FOUND"] = "Yes"
                
    upi_during_period = upi_statement[(upi_statement["COLLECTED DATE"].dt.date >= fromd)] 
    upi_before_period = upi_statement[(upi_statement["COLLECTED DATE"].dt.date < fromd)]         
    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename=UPI Matching.xlsx'
    with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
        upi_during_period[upi_during_period["FOUND"] == "No"].to_excel(writer,sheet_name="Un-Matched UPI (Current)",index=False)
        upi_during_period[upi_during_period["FOUND"] == "Yes"].to_excel(writer,sheet_name="Matched UPI (Current)",index=False)
        upi_before_period[upi_before_period["FOUND"] == "Yes"].to_excel(writer,sheet_name=f"Matched UPI (Before)",index=False)
    
    return response


@api_view(["POST"])
def auto_match_neft(request) : 
    bank_id = request.data.get("bank_id")
    party_id = request.data.get("party_id")
    party_name = models.Party.objects.get(code = party_id).name
    bank_obj = models.BankStatement.objects.get(id = bank_id)
    amt = bank_obj.amt
    allowed_diff = 2
    outstandings = list(models.Outstanding.objects.filter(
        party_id = party_id,
        balance__lte = -1,
        balance__gte = -amt - allowed_diff,
        date__gte = datetime.date.today() - datetime.timedelta(days=60)
    ).values_list("inum","balance"))
    #Try all combination of outstandings whre each row has keys inum and balance.
    #allow if the difference is lesss than allowed_difference with amt
    if len(outstandings) > 20 :
        return JsonResponse({ "status" : "error", "message" : "Too many outstandings to match." })
    match_count = 0
    matched_invoices = []
    for r in range(1, len(outstandings) + 1):
        for combo in combinations(outstandings, r):
            total_balance = sum(-item[1] for item in combo)
            if abs(total_balance - amt) <= allowed_diff:
                matched_invoices = [{"inum": item[0], "balance": -item[1]} for item in combo]
                if match_count == 0 : match_count = 1 
                else : 
                    return JsonResponse({"status" : "error", "message" : "Multiple matches found."})
    if match_count == 0 :
        return JsonResponse({"status" : "error", "message" : "No matching invoices found."})
    else : 
        return JsonResponse({"status": "success", "matched_outstanding": 
                             [ {"bill" : inv["inum"] ,  "party" : party_name , "balance" : inv["balance"] , 
                                "amt" : inv["balance"] } for inv in matched_invoices] 
                })
    


#TODO: Referesh collection (warning : not complete yet)
@api_view(["GET"])
def refresh_bank(request) : 
    sync_reports(limits={"collection": None},min_days_to_sync={"collection" : 15})
    return JsonResponse({"status" : "success"})


