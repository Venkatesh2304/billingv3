
import datetime
from enum import IntEnum
import functools
from io import BytesIO
import json
import threading
import time
from PyPDF2 import PdfReader, PdfWriter
from django.http import FileResponse, HttpResponse, JsonResponse
from openpyxl import load_workbook
import pandas as pd
from app import models, pdf_create
from app.common import bulk_raw_insert, query_db
from app.sync import sync_reports
from custom.classes import Billing, IkeaDownloader
from rest_framework.decorators import api_view
from billingv3.settings import FILES_DIR 

ProcessStatus = IntEnum("ProcessStatus",(("NotStarted",0),("Success",1),("Started",2),("Failed",3)))

class Basepack : 

    basepack_lock = threading.Lock()
    process_logs = []
    ikea: Billing = None
    last_locked_time:float = None 
    beat_export_date:datetime.date = None 

    def current_stock(self) : 
        stock = self.ikea.current_stock(datetime.date.today() )
        stock = stock[stock.Location == "MAIN GODOWN"]
        self.active_basepack_codes = list(set(stock["Basepack Code"].dropna().astype(int))  )
        self.current_stock_original = stock.copy()
    
    def basepack_download(self) : 
        self.basepack_io = self.ikea.basepack()         
    
    def basepack_upload(self) : 
        with open("a.xlsx","wb+") as f : f.write(self.basepack_io.getvalue())
        wb = load_workbook(self.basepack_io , data_only = True)
        sh = wb['Basepack Information']
        rows = sh.values
        basepack = pd.DataFrame( columns=next(rows) , data = rows )
        basepack_original = basepack.copy()
        color_in_hex = [cell.fill.start_color.index for cell in sh['A:A']]
        basepack["color"] = pd.Series( color_in_hex[1:])
        basepack = basepack[ basepack["color"] != 52 ][basepack["BasePack Code"].notna()]
        basepack["new_status"] = basepack["BasePack Code"].isin([ str(code) for code in self.active_basepack_codes ])
        basepack = basepack[ basepack["new_status"] != (basepack["Status"] == "ACTIVE") ]
        basepack.to_excel(f"{FILES_DIR}/basepack.xlsx",index=False,sheet_name="Basepack Information")      
        basepack["Status"] = basepack["Status"].replace({ "ACTIVE" : "INACTIVE_x" , "INACTIVE" : "ACTIVE_x" })
        basepack["Status"] = basepack["Status"].str.split("_").str[0] 
        basepack = basepack[ list(basepack.columns)[5:11] ]
        basepack = basepack.astype({"BasePack Code":str,"SeqNo":int,"MOQ":int})

        output = BytesIO()
        writer = pd.ExcelWriter(output,engine='xlsxwriter')
        basepack.to_excel(writer,index=False,sheet_name="Basepack Information")
        basepack_original.to_excel(writer,index=False,sheet_name="basepack_original")
        self.current_stock_original.to_excel(writer,index=False,sheet_name="currentstock")
        writer.close()
        output.seek(0)

        print( "Basepack Changed (NEW STATUS COUNTS) : " ,  basepack["Status"].value_counts().to_dict() )
        with open(f'{FILES_DIR}/basepack.xlsx', 'wb+') as f:  
            f.write(output.read())
        
        if len(basepack.index) : 
            output.seek(0)
            files = { "file" : ("basepack.xlsx", output ,'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')  }
            res = self.ikea.post("/rsunify/app/basepackInformation/uploadFile", files = files ).text 
            print( res )
            print("Basepack uploaded") 
        else : 
            print("Nothing to upload basepack")

    def beat_export(self) : 
        ##Start Beat Export and Order Sync after basepack uploaded
        today = datetime.date.today() 
        export_data = { "fromDate": str(today) ,"toDate": str(self.beat_export_date) }
        self.ikea.post("/rsunify/app/quantumExport/checkBeatLink",
                data = {'exportData': json.dumps(export_data) })
        self.ikea.post("/rsunify/app/sfmIkeaIntegration/callSfmIkeaIntegrationSync")
        self.ikea.post("/rsunify/app/sfmIkeaIntegration/checkEmpStatus")
        sm = self.ikea.post("/rsunify/app/quantumExport/getSalesmanData", 
                data={"exportData": json.dumps(export_data) }).json()
        sm = ",".join( i[0]  for i in sm )
        self.ikea.post("/rsunify/app/ikeaCommonUtilController/qocRepopulation")
        export_num = self.ikea.post("/rsunify/app/quantumExport/startExport",
                    data = {"exportData": json.dumps(export_data | {"salesManId": sm ,"beatId":"-1"}) } ).json()
        for i in range(60) : 
            status = self.ikea.post("/rsunify/app/quantumExport/getExportStatus",{"processId": export_num}).json()
            if str(status) == str(["0","0","1"]) : #comparing two lists
                print("Beat Export Completed")
                return 
            time.sleep(5)
            self.ikea.logger.debug(f"Waiting for beat export to be completed")
        raise Exception("Beat Export Timed Out After 5 Minutes")

    def order_sync(self) : 
        self.ikea.post("/rsunify/app/sfmIkeaIntegration/callSfmIkeaIntegrationSync")
        self.ikea.post("/rsunify/app/api/callikeatocommoutletcreationallapimethods")
        sync_status = self.ikea.post("/rsunify/app/fileUploadId/upload").text.split("$del")[0]
        self.ikea.logger.debug(f"Order Sync (Basepack) status : {sync_status}")
        
    def basepack(self) :
        #Find beat export date 
        today = datetime.date.today()
        days = 6 
        self.beat_export_date = (today + datetime.timedelta(days=days)) if (today.day <= (20 - days)) or (today.day > 20) else today.replace(day=20)

        if self.basepack_lock.acquire(blocking=False) :
            self.last_locked_time = time.time()
            try : 
                self.process_logs = []
                process_names = ["Current Stock","Basepack Download","Basepack Upload","Beat Export","Order Sync"]
                processes = [self.current_stock,self.basepack_download,self.basepack_upload,self.beat_export,self.order_sync]
                self.process_logs = [{"process" : process_name,"status" : ProcessStatus.NotStarted,"time" : 0} 
                                                        for process_name in process_names ]
                
                self.ikea = Billing()
                for process_name,process_log,process in zip(process_names,self.process_logs,processes,strict=False) : 
                    print(process_name)
                    process_log["status"] = ProcessStatus.Started
                    start_time = time.time()
                    process_failed = False 
                    try :
                        process()
                        process_failed = False
                    except Exception as e :
                        process_failed = True
                        print(f"Error in {process_name} process : {e}")

                    process_log["status"] = (ProcessStatus.Failed if process_failed else  ProcessStatus.Success)
                    end_time = time.time()
                    process_log["time"] = round(end_time - start_time,2)
                    if process_failed : 
                        break
            finally : 
                self.basepack_lock.release()
                self.last_locked_time = None
                print("Basepack Process Completed")
        else : 
            if (time.time() - self.last_locked_time) > 600 : #10 mins over release lock 
                self.basepack_lock.release()
                self.basepack()

basepack_obj = Basepack()

@api_view(["GET","POST"])
def basepack(request) : 
    if request.method == "GET" :
        return JsonResponse(basepack_obj.process_logs,safe=False)
    else : 
        thread = threading.Thread(target=basepack_obj.basepack)
        thread.start()
        return JsonResponse({"status" : "success"})

@api_view(["POST"])
def outstanding(request) :
    TIME_LIMIT = 5*60 
    sync_reports(limits={"sales":TIME_LIMIT,"collection":TIME_LIMIT,"adjustment":TIME_LIMIT})
    
    today = datetime.date.today()
    date = request.data.get("date",today.strftime("%Y-%m-%d"))
    date = datetime.datetime.strptime(date,"%Y-%m-%d").date()
    day = date.strftime("%A").lower()
    outstanding:pd.DataFrame = query_db(f"""
    select salesman_name as salesman , (select name from app_party where party_id = code) as party , beat , inum as bill , 
    (select -amt from app_sales where inum = app_outstanding.inum) as bill_amt , -balance as balance , 
    (select phone from app_party where code = party_id) as phone , 
    round(DATE '{date}' - date) as days , 
    days as weekday 
    from app_outstanding left outer join app_beat on app_outstanding.beat = app_beat.name
    where  balance <= -1 
    """,is_select = True)  # type: ignore 
    IGNORED_PARTIES_FOR_OUTSTANDING = ["SUBASH ENTERPRISES","TIRUMALA AGENCY-P","TIRUMALA AGENCY-D","ANANDHA GENERAL MERCHANT-D-D-D"]
    outstanding = outstanding[~outstanding["party"].isin(IGNORED_PARTIES_FOR_OUTSTANDING)]
    if request.data.get("type") == "wholesale" : outstanding = outstanding[outstanding["beat"].str.contains("WHOLESALE")] 
    if request.data.get("type") == "retail" : outstanding = outstanding[~outstanding["beat"].str.contains("WHOLESALE")] 
    outstanding["coll_amt"] = outstanding["bill_amt"] - outstanding["balance"]
    outstanding = outstanding[["salesman","beat","party","bill","bill_amt","coll_amt","balance","days","phone","weekday"]]
    pivot_fn = lambda df : pd.pivot_table(df,index=["salesman","beat","party","bill"],values=['bill_amt','coll_amt','balance',"days","phone"],aggfunc = "first")[['bill_amt','coll_amt','balance',"days","phone"]] # type: ignore
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    pivot_fn(outstanding[ (outstanding.days >= 21) & outstanding.weekday.str.contains(day) ]).to_excel(writer, sheet_name='21 Days')
    pivot_fn(outstanding[outstanding.days >= 28]).to_excel(writer, sheet_name='28 Days')
    outstanding.sort_values("days",ascending=False).to_excel(writer, sheet_name='ALL BILLS',index=False)
    writer.close()
    output.seek(0)
    with open(f"{FILES_DIR}/outstanding.xlsx","wb+") as f : f.write(output.getvalue())
    return JsonResponse({"status":"success"})


@api_view(["POST"])
def stock_statement(request) : 

    i1 = IkeaDownloader()
    i1.change_user("lakme_rural")
    df1 = i1.current_stock_with_mrp(datetime.date.today())
    df1 = df1[df1["Location"] == "MAIN GODOWN"]

    i2 = IkeaDownloader()
    i2.change_user("lakme_urban")
    df2 = i2.current_stock_with_mrp(datetime.date.today())
    df2 = df2[df2["Location"] == "MAIN GODOWN"]

    details = pd.concat([df1,df2],axis=0).drop_duplicates(subset=["SKU7","MRP"])[["SKU7","Product Name","MRP"]]
    df1 = df1.groupby(["SKU7","MRP"])[["Units"]].sum().reset_index()
    df2 = df2.groupby(["SKU7","MRP"])[["Units"]].sum().reset_index()
    df = df1.merge(df2, on=["SKU7","MRP"], how="outer", suffixes=(" Rural"," Urban")).fillna(0)
    df["Total Qty"] = df["Units Rural"] + df["Units Urban"]
    df = df.merge(details, on=["SKU7","MRP"], how="left")
    df = df[["SKU7","Product Name","MRP","Units Rural","Units Urban","Total Qty"]]
    #send the df as excel in django 
    df.to_excel(f"{FILES_DIR}/stock_statement.xlsx", index=False, sheet_name='Current Stock')
    return JsonResponse({"status":"success"})


@api_view(["POST"])
def pending_sheet(request) :
    date = request.data.get("date")
    beat_type = request.data.get("type")
    date = datetime.datetime.strptime(date,"%Y-%m-%d").date()
    queryset = models.Beat.objects.filter(days__contains = date.strftime("%A").lower())
    if beat_type == "retail" : 
        queryset = queryset.exclude(name__contains = "WHOLESALE")
    else :
        queryset = queryset.filter(name__contains = "WHOLESALE")
    
    #Filter queryset if beats is not empty in POST
    beats = request.data.get("beats")
    if beats :
        queryset = queryset.filter(id__in = beats)
    
    beat_ids = { str(id) for id in queryset.values_list("id",flat=True) }
    beat_maps = { beat.name : (beat.salesman_name,beat.id) for beat in queryset.all() }
    billing = Billing()
    bytesio = billing.pending_statement_excel(beat_ids,date - datetime.timedelta(days=1)) #Dont consider bills on the same date
    df = pd.read_excel(bytesio,skiprows = 13,skipfooter=1)
    df = df.dropna(subset = "Beat Name")
    df["Salesperson Name"] = df["Salesperson Name"].str.split("-").str[1]
    pdfs = [] 
    for beat , rows in df.groupby("Beat Name") : 
        max_days_per_party = rows.groupby("Party Name")["Bill Ageing (In Days)"].max().to_dict()
        rows["max_days_per_party"] = rows["Party Name"].map(max_days_per_party)
        rows = rows.sort_values(by = ["max_days_per_party","Party Name"] , ascending=False)
        salesman,beat_id = beat_maps[beat]
        sheet_no = "PS" + date.strftime("%d%m%y") + str(beat_id)
        models.PendingSheet(sheet_no=sheet_no,beat=beat,salesman=salesman,date=date).save()
        rows["sheet_id"] = sheet_no 
        renamed_rows = rows.rename(columns={"Bill No":"bill_id","OutstANDing Amount":"outstanding_amt","Bill Ageing (In Days)" : "days"})
        bulk_raw_insert("pendingsheetbill",renamed_rows[["sheet_id","bill_id","days","outstanding_amt"]],ignore=True,index=["sheet_id","bill_id"])
        bytesio = pdf_create.pending_sheet_pdf(rows , sheet_no ,  salesman , beat , date)
        pdfs.append(bytesio)
    
    writer = PdfWriter()
    for pdf in pdfs :
        reader = PdfReader(pdf)
        for page in reader.pages:
            writer.add_page(page)
        if len(reader.pages) % 2 != 0:
            writer.add_blank_page()
    writer.write(f"{FILES_DIR}/pending_sheet.pdf")
    return JsonResponse({"status":"success"})