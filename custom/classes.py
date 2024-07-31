from collections import defaultdict
from io import BytesIO
import json
import random
import numpy as np
from datetime import datetime,timedelta
import pandas as pd 
import base64
from pathlib import Path
import zipfile
from dateutil.parser import parse as date_parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import re
from pathlib import Path    
from multiprocessing.pool import ThreadPool
from tqdm import tqdm
from urllib.parse import parse_qsl

from .curl import get_curl , curl_replace 
from .Session import Session,StatusCodeError
from bs4 import BeautifulSoup

class IkeaPasswordExpired(Exception) :
    pass

class IkeaWrongCredentails(Exception) :
    pass

class BaseIkea(Session) : 
          
      key = "ikea"
      IKEA_GENERATE_REPORT_URL = "/rsunify/app/reportsController/generatereport"
      IKEA_DOWNLOAD_REPORT_URL = "/rsunify/app/reportsController/downloadReport?filePath="
      
      def date_epochs(self) :
        return int((datetime.now() - datetime(1970, 1, 1)
                        ).total_seconds() * 1000) - (330*60*1000)
   
      def report(self,key,pat,replaces,fname = None,is_dataframe =False) :
          r  = get_curl(key)
          if isinstance(r.data,str) :
             r.data = dict(parse_qsl( r.data ))
          if "jsonObjWhereClause" in r.data :
                r.data['jsonObjWhereClause'] =  curl_replace(  pat , replaces ,  r.data['jsonObjWhereClause'] )
                del r.data['jsonObjforheaders']
          durl = r.send(self).text
          if durl == "" : return None 
          res = self.download_file( durl , fname )
          return pd.read_excel(res) if is_dataframe else res 

      def download_file(self,durl,fname = None) -> BytesIO:
          if durl == "" :
              raise Exception("Download URL is empty") 
          response_buffer = self.get_buffer(durl)
          if fname is not None :
             with open(fname, "wb+") as f:
                f.write(response_buffer.getbuffer())          
          return response_buffer 
      
      def download_dataframe(self,key,skiprows,sheet=None) -> pd.DataFrame : 
          kwargs = {} if sheet is None else {"sheet_name":sheet}
          durl = get_curl(key).send(self).text
          return pd.read_excel( self.get_buffer(durl) , skiprows = skiprows , **kwargs )
       
      def is_logged_in(self) : 
         try : 
           self.get("/rsunify/app/billing/getUserId")
           self.logger.info("Login Check : Passed")
           return True 
         except StatusCodeError : 
           self.logger.error("Login Check : Failed")
           return False 
      
      def login(self) : 
          self.logger.info("Login Initiated")
          self.cookies.clear()
          time_epochs = self.date_epochs()
          preauth_res_text = self.post("/rsunify/app/user/authentication",data={'userId': self.config["username"] , 'password': self.config["pwd"], 'dbName': self.config["dbName"], 'datetime': time_epochs , 'diff': -330}).text
          if "CLOUD_LOGIN_PASSWORD_EXPIRED" == preauth_res_text : 
             raise IkeaPasswordExpired
          elif "<body>" in preauth_res_text : 
             raise IkeaWrongCredentails
          else : 
             pass 
          response = self.post("/rsunify/app/user/authenSuccess",{})
          if response.status_code == 200 : 
             self.logger.info("Logged in successfully")
             self.db.update_cookies(self.cookies)
          else : 
             raise Exception("Login Failed")

      def __init__(self) : 
          super().__init__()
          self.base_url = self.config["home"]
          self.headers.update({'accept': 'application/json, text/javascript, */*; q=0.01'})
          while not self.is_logged_in() :  
             self.login()

      def get_buffer(self,relative_url) : 
          return super().get_buffer(self.IKEA_DOWNLOAD_REPORT_URL + relative_url)
      
      def parllel(self,fn,list_of_args,max_workers=10,show_progress=False,is_async=False) : 
          pool = ThreadPool(max_workers) 
          list_of_args = [ [self] + list(args) for args in list_of_args ]
          if show_progress : 
             pbar = tqdm(total = len(list_of_args)) 
          def progress_function(*args) : 
              fn(*args)
              pbar.update(1) 
          overloaded_fn = progress_function if show_progress else fn
          results =  pool.starmap_async(overloaded_fn,list_of_args) if is_async else pool.starmap(overloaded_fn,list_of_args) 
          return results

  
      
class IkeaDownloader(BaseIkea) :  

      def gstr(self,fromd,tod,gstr_type=1) -> BytesIO :
          r = get_curl("ikea/gstr")
          r.url = re.sub(r"pramFromdate=(.{10})",fromd.strftime("pramFromdate=%d/%m/%Y"),r.url)
          r.url = re.sub(r"paramToDate=(.{10})",tod.strftime("paramToDate=%d/%m/%Y"),r.url)
          r.url = re.sub(r"gstrValue=(.{1})",str(gstr_type),r.url)
          durl = r.send(self).text
          return self.download_file( durl , f"gst{gstr_type}.csv" )
        
      def outstanding(self,date:datetime.date) -> dict : 
          return self.report("ikea/outstanding",r'(":val9":").{10}(.{34}).{10}', (date.strftime("%Y-%m-%d"),date.strftime("%Y-%m-%d")) , is_dataframe = True ) 
      
      def pending_bills(self,date:datetime.date) -> dict : 
          return self.report("ikea/pending_bills",r'(":val8":").{10}', (date.strftime("%Y-%m-%d"),) , is_dataframe = True ) 
      
      def beat_mapping(self) -> dict : 
          return self.report("ikea/beat_mapping","","",is_dataframe = True ) 
      
      def product_hsn(self) -> dict : 
          return get_curl("ikea/list_of_products").send(self).json() 
      
      def party_master(self) -> pd.DataFrame : 
          return self.download_dataframe("ikea/party_master",skiprows=9)
      
      def stock_master(self) -> pd.DataFrame : 
          return self.download_dataframe("ikea/stock_master",skiprows=9)
      
class Billing(BaseIkea) :
    today = datetime.date( datetime.now() )
    order_date = datetime.date( datetime.now() - timedelta(days=1) )
    lines = 100
    lines_count = {}
    creditrelease = {}
    
    def __init__(self,max_lines,creditrelease,prev_collection,lines_count):
        super().__init__()
        self.lines = max_lines
        self.lines_count = lines_count
        self.prev_collection = prev_collection
        self.creditrelease = creditrelease
        self.thread_executor = ThreadPoolExecutor()
        self.plg_thread = self.thread_executor.submit(self.get_plg_maps)
        
    def client_id_generator(self): 
        return np.base_repr(self.date_epochs(), base=36).lower() + np.base_repr(random.randint(pow(10, 17), pow(10, 18)),
                 base=36).lower()[:11]
        
    def get_plg_maps(self) :
        soup = BeautifulSoup( self.get("/rsunify/app/rssmBeatPlgLink/loadRssmBeatPlgLink").text ) 
        plg_maps = soup.find("input", {"id" : "hiddenSmBeatLnkMap"}).get("value") 
        plg_maps = sum( list(json.loads(plg_maps).values()) ,start=[] )
        plg_maps = pd.DataFrame(plg_maps).astype({ 0  : int })
        self.logger.log_dataframe(plg_maps)
        return plg_maps 
    
    def get_collection_report(self) -> pd.DataFrame :
        today = self.today.strftime("%Y/%m/%d")
        return self.report("ikea/collection_report" , r'(":val10":").{10}(",":val11":").{10}(",":val12":"2018/04/01",":val13":").{10}', 
                    (today,) * 3 , is_dataframe=True)
    
    def get_creditlock(self,party_data) : 
        get_crlock_url = f'/rsunify/app/billing/partyplgdatas?partyCode={party_data["partyCode"]}&parCodeRef={party_data["parCodeRef"]}&parHllCode={party_data["parHllCode"]}&plgFlag=true&salChnlCode=&showPLG={party_data["showPLG"]}&isMigration=true'
        return self.get(get_crlock_url).json()
    
    def release_creditlock(self, party_data):
        return 
        party_credit = self.get_creditlock(party_data)
        set_url = f'/rsunify/app/billing/updatepartyinfo?partyCodeRef={party_data["partyCode"]}&creditBills={int(party_credit["creditBillsUtilised"])+1}&creditLimit={party_credit["creditLimit"]}&creditDays=0&panNumber=&servicingPlgValue={party_data["showPLG"]}&plgPartyCredit=true&parHllCode={party_data["parHllCode"]}'
        self.get(set_url)

    def get_party_phone_number(self,party_code) : 
        party_data =  self.get(f"/rsunify/app/partyMasterScreen/retrivePartyMasterScreenData?partyCode={party_code}").json()
        return party_data["partydetails"][0][16]
    
    def get_party_outstanding_bills(self, party_data):
        res = self.get_creditlock(party_data)
        outstanding = res["collectionPendingBillVOList"]
        breakup = [[bill["pendingDays"], bill["outstanding"]] for bill in outstanding]
        breakup.sort(key=lambda x: x[0], reverse=True)
        breakup = "/".join([str(bill[0])+"*"+str(bill[1]) for bill in breakup])
        return {"billsutilised": res["creditBillsUtilised"], "bills" : breakup}
    
    def interpret(self, cr_lock_parties):
        ## Find the beat to plg map 
        plg_maps = self.plg_thread.result()
        coll_report = self.collection_report_thread.result() # Can be None if no collection 

        ## Collection Report 
        if coll_report is not None :
           coll_report["party"] = coll_report["Party Name"].str.replace(" ","")
           coll_report = coll_report[~coll_report.Status.isin(["PND","CAN"])]
           coll_report = coll_report.dropna(subset="Collection Date")
           coll_report["Collection Date"] = pd.to_datetime( coll_report["Collection Date"] , format="%d/%m/%Y" )
           coll_report["days"] = (coll_report["Collection Date"] - coll_report["Date"]).dt.days
           self.logger.log_dataframe( coll_report , "Collection Report")
        
        creditlock = {}
        def prepare_party_data(self: Billing,party) : 
            creditlock[party] = party_data = cr_lock_parties[party]
            plg_name = plg_maps[plg_maps[0] == party_data["beatId"]].iloc[0][2]
            beat_name = plg_maps[plg_maps[0] == party_data["beatId"]].iloc[0][1]
            party_data["showPLG"] = plg_name.replace("+", "%2B")
            party_data["beat_name"] = beat_name
            lock_data = self.get_party_outstanding_bills(party_data)
            party_data["billsutilised"] = lock_data["billsutilised"]
            party_data["bills"] = lock_data["bills"]
            coll_str = "No Collection"
            if coll_report is not None : 
               coll_data = coll_report[coll_report.party == party]
               if len(coll_data.index)  :  
                  coll_str = "/".join( f'{round(row["days"].iloc[0])}*{ round(row["Coll. Amt"].sum()) }' for billno,row in coll_data.groupby("Bill No") ) 
            
            party_data["coll_str"] = coll_str
            party_data["ph"] = self.get_party_phone_number(party_data['partyCode'])

        self.parllel(prepare_party_data , zip(cr_lock_parties))
        self.logger.info(f"CreditLock :: \n{creditlock}")
        return creditlock
    
    def Sync(self): 
        return self.post('/rsunify/app/fileUploadId/download')

    def Prevbills(self):
        delivery_req = get_curl("ikea/billing/getdelivery")
        delivery = delivery_req.send(self).json()["billHdBeanList"] or []
        self.prevbills = [ bill['blhRefrNo'] for bill in delivery ]
        self.logger.info(f"Previous Delivery :: {self.prevbills}")

    def Collection(self):
        
        self.parllel( Billing.release_creditlock , ((party_data,) for party, party_data in self.creditrelease.items()) )
        self.get("/rsunify/app/quantumImport/init")
        self.get("/rsunify/app/quantumImport/filterValidation")
        self.get(f"/rsunify/app/quantumImport/futureDataValidation?importDate={self.today.strftime('%d/%m/%Y')}")

        self.import_dates = {"importDate": (self.today - timedelta(days=1)).strftime("%Y-%m-%d") + "T18:30:00.000Z",
                             "orderDate": (self.order_date - timedelta(days=1)).strftime("%Y-%m-%d") + "T18:30:00.000Z"}
        get_collection_req = get_curl("ikea/billing/getmarketorder")
        get_collection_req.url = self.base_url + "/rsunify/app/quantumImport/validateloadcollection"
        get_collection_req.json |= self.import_dates 
        self.market_collection = get_collection_req.send(self).json()
        self.get("/rsunify/app/quantumImport/processcheck")
        
        collection_data = self.market_collection["mcl"]
        for coll in collection_data : 
            coll["ck"] = (coll["pc"] not in self.prev_collection)
            coll["bf"] = True
        self.pushed_collection_ids = [ coll["pc"] for coll in collection_data if coll["ck"]  ]

        coll_payload = {"mcl": collection_data, "id": self.today.strftime( "%d/%m/%Y"), "CLIENT_REQ_UID": self.client_id_generator() , "ri" : 0 }
        self.logger.info(f"Imported Collection :: {self.pushed_collection_ids}")
        postcollection = self.post("/rsunify/app/quantumImport/importSelectedCollection", json=coll_payload).json()
        self.collection_report_thread = self.thread_executor.submit(self.get_collection_report)
        
    def Order(self):
        get_shikhar = get_curl("ikea/billing/getshikhar")
        get_shikhar.json["importDate"] =  self.today.strftime("%d/%m/%Y")
        shikhar_data = get_shikhar.send(self).json()["shikharOrderList"]
        shikhar_ids = [order[11] for order in shikhar_data[1:]]
    
        get_order_req = get_curl("ikea/billing/getmarketorder")
        get_order_req.json |= (self.import_dates | {"qtmShikharList" : shikhar_ids})
        self.market_order = get_order_req.send(self).json()
    
        order_data = self.market_order["mol"]
        self.order_original = pd.DataFrame(order_data)
        orders = self.order_original.groupby("on", as_index=False)
        orders = orders.filter(lambda x: all([x.on.count() <= self.lines,
                                      x.on.iloc[0] not in self.lines_count or self.lines_count[x.on.iloc[0]] == x.on.count(),
            "WHOLE" not in x.m.iloc[0] ,
            (x.t * x.cq).sum() > 100 ,
            # x.aq.sum() > 0 
        ]))

        self.logger.log_dataframe(orders,"Filtered orders : ")      
        billwise_lines_count = orders.groupby("on")["cq"].count().to_dict()
        self.logger.info(f"Bill Wise Line Count : {billwise_lines_count}") #Need to Prettify

        orders["billvalue"], orders["status"] = orders.t * orders.cq , False
        orders.p = orders.p.apply(lambda x: x.replace(" ", "")) # party spacing problem prevention
        orders["on_str"] = orders.on.astype(str) + ","
        cr_lock_parties = orders.groupby("on").filter(lambda x :  ("Credit Exceeded" in x.ar.values) ).groupby("p").agg(
                         {"pc": "first", "ph": "first", "pi": "first", "s": "first", "billvalue": "sum", "mi":  "first" , "on_str" : "sum"})
        cr_lock_parties.rename(columns={"pc": "partyCode", "ph": "parHllCode","s": "salesman", "pi": "parId", "mi": "beatId"}, inplace=True)
        cr_lock_parties["billvalue"]  = cr_lock_parties["billvalue"].round(2)
        cr_lock_parties["parCodeRef"] = cr_lock_parties["partyCode"].copy()
        cr_lock_parties["orders"] = cr_lock_parties["on_str"].apply(lambda x: list(set( x.split(",")[:-1] ))  )
        del cr_lock_parties["on_str"]


        self.creditlock = self.interpret(cr_lock_parties.to_dict(orient="index"))

        for order in order_data :
            order["ck"] = (order["on"] in orders.on.values)

        uid = self.client_id_generator()
        post_market_order = get_curl("ikea/billing/postmarketorder")
        post_market_order.json |= {"mol": order_data , "id": self.today.strftime("%d/%m/%Y"), "CLIENT_REQ_UID": uid}
        

        log_durl = post_market_order.send(self).json()["filePath"]
        log_file = self.get_buffer(log_durl).read().decode()  # get text from string

        self.interpret(cr_lock_parties.to_dict(orient="index"))
        #return self.creditlock_data

    def Delivery(self):
        delivery = get_curl("ikea/billing/getdelivery").send(self).json()["billHdBeanList"] or []
        if len(delivery) == 0 : 
           self.bills = []
           return 
        delivery = pd.DataFrame(delivery)
        self.logger.debug(f"All Delivery Bills :: {list(delivery.blhRefrNo)}")
        delivery = delivery[ ~delivery.blhRefrNo.isin(self.prevbills) ]
        self.bills = list(delivery.blhRefrNo)
        self.logger.info(f"Final Bills :: {self.bills}")
        delivery["vehicleId"] = 1
        data = {"deliveryProcessVOList": delivery.to_dict(orient="records"), "returnPickList": []}
        self.post("/rsunify/app/deliveryprocess/savebill",json=data).json()
        #self.prev_bills += self.bills

    def Download(self):
        if len(self.bills) == 0 : return
        get_bill_durl = lambda billfrom,billto,report_type : self.get(f"/rsunify/app/commonPdfRptContrl/pdfRptGeneration?strJsonParams=%7B%22billFrom%22%3A%22{billfrom}%22%2C%22billTo%22%3A%22{billto}%22%2C%22reportType%22%3A%22{report_type}%22%2C%22blhVatFlag%22%3A2%2C%22shade%22%3A1%2C%22pack%22%3A%22910%22%2C%22damages%22%3Anull%2C%22halfPage%22%3A0%2C%22bp_division%22%3A%22%22%2C%22salesMan%22%3A%22%22%2C%22party%22%3A%22%22%2C%22market%22%3A%22%22%2C%22planset%22%3A%22%22%2C%22fromDate%22%3A%22%22%2C%22toDate%22%3A%22%22%2C%22veh_Name%22%3A%22%22%2C%22printId%22%3A0%2C%22printerName%22%3A%22TVS+MSP+250+Star%22%2C%22Lable_position%22%3A2%2C%22billType%22%3A2%2C%22printOption%22%3A%220%22%2C%22RptClassName%22%3A%22BILL_PRINT_REPORT%22%2C%22reptName%22%3A%22billPrint%22%2C%22RptId%22%3A%22910%22%2C%22freeProduct%22%3A%22Default%22%2C%22shikharQrCode%22%3Anull%2C%22rptTypOpt%22%3A%22pdf%22%2C%22gstTypeVal%22%3A%221%22%2C%22billPrint_isPrint%22%3A0%2C%22units_only%22%3A%22Y%22%7D").text
     
        self.billfrom, self.billto = self.bills[0],  self.bills[-1]
        self.download_file( get_bill_durl(self.billfrom,self.billto,"pdf") , "bill.pdf" )
        self.download_file( get_bill_durl(self.billfrom,self.billto,"txt") , "bill.txt" )
        
    def Printbill(self, print_type={"original": 0, "duplicate": 0}):
        if len(self.bills) == 0 : return
        secondarybills.main('bill.txt', 'bill.docx')
        try:
            import win32api
            for i in range(0, print_type["duplicate"]):
                win32api.ShellExecute(0, 'print', 'bill.docx', None, '.', 0)
            for i in range(0, print_type["original"]):
                win32api.ShellExecute(0, 'print', "bill.pdf", None, '.', 0)
        except Exception as e:
            print("Win32 Failed . Printing Failed")

## Needs to checked 
class Gst(Session) : 
     key = "gst"
     fetch_user_data =  lambda self :  self.get("https://services.gst.gov.in/services/api/ustatus").json()
     base_url = "https://gst.gov.in"

     def __init__(self) : 
          self.db = db
          self.home = "https://gst.gov.in"
          self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.115 Safari/537.36" }
          super().__init__()
          base_path = Path(__file__).parent
          self.dir = str( (base_path / ("data/gst/" + self.user_config["dir"])).resolve() )
          self.rtn_types_ext = {"gstr1":"zip","gstr2a":"zip","gstr2b":"json"}
     
     def captcha(self) : 
          self.cookies.clear()
          self.get('https://services.gst.gov.in/services/login')
          login = self.get('https://services.gst.gov.in/pages/services/userlogin.html')
          captcha = self.get('https://services.gst.gov.in/services/captcha?rnd=0.7395713643528166').content
          self.db.update_cookies(self.cookies)
          return captcha
          
     def login(self,captcha) :
          data =  { "captcha": captcha , "deviceID": None ,"mFP": "{\"VERSION\":\"2.1\",\"MFP\":{\"Browser\":{\"UserAgent\":\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.115 Safari/537.36\",\"Vendor\":\"Google Inc.\",\"VendorSubID\":\"\",\"BuildID\":\"20030107\",\"CookieEnabled\":true},\"IEPlugins\":{},\"NetscapePlugins\":{\"PDF Viewer\":\"\",\"Chrome PDF Viewer\":\"\",\"Chromium PDF Viewer\":\"\",\"Microsoft Edge PDF Viewer\":\"\",\"WebKit built-in PDF\":\"\"},\"Screen\":{\"FullHeight\":864,\"AvlHeight\":816,\"FullWidth\":1536,\"AvlWidth\":1536,\"ColorDepth\":24,\"PixelDepth\":24},\"System\":{\"Platform\":\"Win32\",\"systemLanguage\":\"en-US\",\"Timezone\":-330}},\"ExternalIP\":\"\",\"MESC\":{\"mesc\":\"mi=2;cd=150;id=30;mesc=739342;mesc=770243\"}}" ,
                    "password": self.config["pwd"] , "type": "username" , "username": self.config["username"] }
          res = self.post("https://services.gst.gov.in/services/authenticate" ,headers = {'Content-type': 'application/json'},json = data).json()
          if "errorCode" in res.keys() : 
              if res["errorCode"] == "SWEB_9000" : 
                 return False 
              elif res["errorCode"] == "AUTH_9002" : 
                  raise Exception("Invalid Username or Password")
              elif res["errorCode"] == "AUTH_9033" : 
                  raise Exception("Password Expired , kindly change password")
              else : 
                  raise Exception("Unkown Exception")
          auth =  self.get("https://services.gst.gov.in/services/auth/",headers = {'Referer': 'https://services.gst.gov.in/services/login'})
          self.db.update_cookies(self.cookies)
     
     def is_logged_in(self) : 
         return len(self.getuser()) != 0 

     def getuser(self) : 
           data = self.get("https://services.gst.gov.in/services/api/ustatus",
           headers = {"Referer": "https://services.gst.gov.in/services/auth/fowelcome"}).json()
           return data 
     
     def getinvs(self,period,types,gstr_type="gstr1") :
         uploaded_by = 'OE' if 'B2CS' in types.upper()  else 'SU'
         data = self.get(f"https://return.gst.gov.in/returns/auth/api/{gstr_type}/invoice?rtn_prd={period}&sec_name={types.upper()}&uploaded_by={uploaded_by}",
                        headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr1"}).json()
         if "error" in data.keys()  :
             return []
         invs = data["data"]["processedInvoice"]
         return invs 
     
     def multi_downloader(self,periods,rtn_type="gstr1") :  
         """User function to download zips / jsons for multi period and different rtn_types"""
         rtn_type = rtn_type.lower()
         downloader_functions = {"zip":self.download_zip,"json":self.download_json}
         fname_ext = self.rtn_types_ext[rtn_type]
         downloader_function = downloader_functions[fname_ext]
         dir = self.dir + "/" + rtn_type
         downloads = []
         with ThreadPoolExecutor(max_workers=9) as executor:
              for period in periods :
                  if not os.path.exists(f"{dir}/{period}.{fname_ext}") : 
                         downloads.append(executor.submit( downloader_function,period,dir,rtn_type))
              for future in as_completed(downloads): pass 

     def download_zip(self,period,dir,rtn_type) :
         get_status = lambda flag : self.get(f"https://return.gst.gov.in/returns/auth/api/offline/download/generate?flag={flag}&rtn_prd={period}&rtn_typ={rtn_type.upper()}",
                                    headers={"Referer":"https://return.gst.gov.in/returns/auth/gstr/offlinedownload"}).json()     
         while True :  
             print(period,"looping..")
             try : status = get_status(0)
             except : 
                time.sleep(60) 
                continue
             print(status) 
             if "data" in  status and "token" not in status["data"] : #already download generated 
                 if datetime.now() - date_parse(status["data"]["timeStamp"])  >= timedelta(hours=24) : 
                    get_status(1)
                 else : 
                    os.makedirs(dir,exist_ok=True)
                    with open(f"{dir}/{period}.zip","wb+") as f : 
                        f.write( self.get( status["data"]["url"][0] ).content )
                        print(f"{period} donwloaded...")
                    break 
             time.sleep(60)

     def download_json(self,period,dir,rtn_type) :  
        os.makedirs(dir,exist_ok=True)
        data = self.get(f"https://gstr2b.gst.gov.in/gstr2b/auth/api/gstr2b/getjson?rtnprd={period}",
                    headers = {"Referer": "https://gstr2b.gst.gov.in/gstr2b/auth/"}).json()
        if "error" in data : 
            if data["error"]["error_cd"] == "RET2B1016" : data = {}
            else : 
                print(data) 
                raise Exception("Error on Download Json")
        else  : 
            data = data["data"]["docdata"]            
        json.dump( data , open(f"{dir}/{period}.json","w+") )
          
     def read_json(self,period,rtn_type,dir=None) :
         fname_ext = self.rtn_types_ext[rtn_type]
         if dir is None : dir = self.dir 
         dir = dir + "/" + rtn_type
         fname = f"{dir}/{period}.{fname_ext}"
         json_file = fname
         if not os.path.exists(fname) : return None 

         if fname_ext == "zip" : 
            json_file = zipfile.ZipFile(fname).namelist()[0]
            os.system(f"unzip -o {fname}")

         data = defaultdict(list , json.load( open(json_file) ) )
         dfs = {}
         for (type,key) in [("b2b","inv"),("cdnr","nt")] : 
             if rtn_type in ["gstr1"] :  
                df  = pd.DataFrame( [  j | k["itm_det"] | {"ctin":i["ctin"]}  for i in data[type] for j in i[key] for k in j["itms"] ] )
                if len(df.index) : del df["itms"]
             if rtn_type in ["gstr2a","gstr2b"] :
                df  = pd.DataFrame( [  j | k | {"ctin":i["ctin"]}  for i in data[type] for j in i[key] for k in j["items"] ] )
                if len(df.index) : del df["items"]
             df["period"] = period 
             dfs[type] = df
         for type in ["b2cs"] :
             df = pd.DataFrame( data["b2cs"] ) 
             df["period"] = period 
             dfs["b2cs"] = df
         dfs["period"] = period
         return dfs 
     
     def make_report(self,periods,rtn_type,dir_report,filter_func=None,) :
         data = [ self.read_json(month,rtn_type) for month in periods  ]
         data = [ i for i in data if i is not None ]         
         agg = {"txval":sum,"camt":sum,"samt":sum}
         all = []
         for (k,inum_column) in [("b2b","inum"),("cdnr","nt_num"),("b2cs","rt")] :
             df = pd.concat([ i[k] for i in data ] ,axis=0)
             if len(df.index) == 0 : continue 
             if filter_func is not None : 
                if k not in filter_func : continue 
                df = filter_func[k](df)
             t = pd.to_datetime(df['period'],format="%m%Y").dt.to_period('Q-OCT').dt
             if rtn_type in ["gstr2b"] : 
                 df = df.rename(columns={"cgst":"camt","sgst":"samt","ntnum":"nt_num"})

             df["year"] = (t.qyear-1).astype(str) + "-" + t.qyear.astype(str)
             df["count"] = df[inum_column]
             if "nt_num" in df.columns : df = df.rename(columns = {"nt_num" : "inum"})

             writer = pd.ExcelWriter(f"{dir_report}/{rtn_type}_{k}.xlsx") 
             df.groupby("period").aggregate(agg | {"count":"nunique"}).to_excel( writer , sheet_name="Monthly")
             df.groupby("year").aggregate(agg | {"count":"nunique"}).to_excel( writer , sheet_name="Yearly")
             if "ctin" in df.columns : 
                 df_party_sum = df.pivot_table(index=["ctin","period"] , values = agg.keys() , aggfunc=agg, margins=True)
                 df_party_sum.to_excel( writer , sheet_name="Party-Wise")
             df.to_excel(writer,sheet_name="Detailed",index=False)
             writer.close()
             df["type"] = k 
             all.append( df[["period","year","txval","camt","samt","type"]] )
         all = pd.concat(all,axis=0)
         writer = pd.ExcelWriter(f"{dir_report}/{rtn_type}_all.xlsx") 
         all.groupby(["period","type"]).aggregate(agg).to_excel( writer , sheet_name="Monthly")
         all.groupby(["year","type"]).aggregate(agg).to_excel( writer , sheet_name="Yearly")
         all.to_excel(writer,sheet_name="Detailed",index=False)
         writer.close()   
     
     def get_einv_data(self,seller_gstin,period,doctype,inum) : 
         p = datetime.strptime( "01" + period , "%d%m%Y" )
         year = (p.year - 1) if p.month < 4 else p.year 
         fy = f"{year}-{(year+1)%100}"
         params = {'stin': seller_gstin ,'fy': fy ,'doctype': doctype ,'docnum': str(inum) ,'usertype': 'seller'}
        #  params = {"usertype": "seller","irn" : irn}
         data = self.get('https://einvoice.gst.gov.in/einvoice/auth/api/getIrnData',
             params=params, headers = { 'Referer': 'https://einvoice.gst.gov.in/einvoice/jsonDownload' }
         ).json()
         if "error" in data : return None     
         data = json.loads(data["data"])["data"]
         signed_inv = data["SignedInvoice"]
         while len(signed_inv) % 4 != 0: signed_inv += "="
         payload = base64.b64decode(signed_inv.split(".")[1] + "==").decode("utf-8")
         inv = json.loads( json.loads(payload)["data"] )
         qrcode = data["SignedQRCode"]
         return inv | { "qrcode" : qrcode }
        #  return signed_inv 
     
     def upload(self,period,fname) : 
           print(fname)
           input(self.fetch_user_data()["bname"])
           files = {'upfile': ( "gst.json" , open(fname) , 'application/json', { 'Content-Disposition': 'form-data' })}
           ret_ref = {"Referer": "https://return.gst.gov.in/returns/auth/gstr/offlineupload"}
           ref_id =  self.post(f"https://return.gst.gov.in/returndocs/offline/upload",
                  headers = ret_ref | {"sz" : "304230" }, 
                  data = {  "ty": "ROUZ" , "rtn_typ": "GSTR1" , "ret_period": period } ,files=files).json()
           ref_id = ref_id['data']['reference_id']
           res = self.post("https://return.gst.gov.in/returns/auth/api/gstr1/upload" , headers = ret_ref,
                           json = {"status":"1","data":{"reference_id":ref_id},"fp":period}) 
       
           for times in range(0,90) : 
              time.sleep(1)
              status_data = self.get(f"https://return.gst.gov.in/returns/auth/api/offline/upload/summary?rtn_prd={period}&rtn_typ=GSTR1",
                       headers = ret_ref).json()["data"]["upload"] 
              for status in status_data : 
                  if status["ref_id"] == ref_id : 
                     print( status )
                     if status["status"] == "PE" : 
                         self.get(f" https://return.gst.gov.in/returns/auth/api/offline/upload/error/generate?ref_id={ref_id}&rtn_prd={period}&rtn_typ=GSTR1",headers = ret_ref)
                     return status     

     def get_error(self,period,ref_id,fname) : 
         for times in range(0,40) : 
            time.sleep(1)
            res = self.get(f"https://return.gst.gov.in/returns/auth/api/offline/upload/summary?rtn_prd={period}&rtn_typ=GSTR1",
                     headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr/offlineupload"}).json()  
            status_data = res["data"]["upload"]
            for status in status_data : 
                if status["ref_id"] == ref_id :
                  if status["er_status"] == "P" : 
                    res = self.get(f"https://return.gst.gov.in/returns/auth/api/offline/upload/error/report/url?token={status['er_token']}&rtn_prd={period}&rtn_typ=GSTR1",
                              headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr/offlineupload"}) 
                    with open(fname,"wb") as f  : 
                          f.write(res.content) 
                    return None 
         raise Exception("GST Get error timed out")           

### DEPRECEATED
# def myHash(str) : 
#   hash_object = hashlib.md5(str.encode())
#   md5_hash = hash_object.hexdigest()
#   return hashlib.sha256(md5_hash.encode()).hexdigest()

# def parseEwayExcel(data) : 
#     err_map = { "No errors" : lambda x : x == "" , "Already Generated" :  lambda x : "already generated" in x }
#     err_list = defaultdict(list)
#     for bill in data.iterrows() : 
#         err = bill[1]["Errors"]
#         Type = None
#         for err_typ , err_valid in err_map.items() : 
#             if type(err) == str and err_valid(err) :
#                Type = err_typ 
#                break 
#         if Type == None : 
#            Type = "Unknown error"
#         err_list[Type].append( [ bill[1]["Doc No"] , err  ])
#     return err_list

# class ESession(Session) : 
#       def __init__(self,key,home,_user,_pwd,_salt,_captcha) :  
#           self.key = key 
#           self.db = db 
#           self.home = home 
#           super().__init__()
#           self._captcha  = True 
#           self._captcha_field = _captcha
#           self.headers.update({ "Referer": home })
#           if not hasattr(self,"form") : self.form = {} 
#           else : 
#             self.form = json.loads(self.form.replace("'",'"')) if isinstance(self.form,str) else self.form 
#             self.hash_pwd = hashlib.sha256((myHash(self.pwd) + self.form[_salt]).encode()).hexdigest()          
#             self.form[_pwd]  , self.form[_user]  = self.hash_pwd , self.username
    
#           self._login_err = (   lambda x : (x.url == "https://einvoice1.gst.gov.in/Home/Login" , x.text) ,
#                                 [( lambda x : x[0] and "alert('Invalid Login Credentials" in x[1]  , {"status" : False , "err" : "Wrong Credentials"} ) , 
#                                 ( lambda x :  x[0] and "alert('Invalid Captcha" in x[1]  , {"status" : False , "err" : "Wrong Captcha"} ) ,
#                                 ( lambda x :  x[0] and True  , {"status" : False , "err" : "Unkown error"} )] )

# class Einvoice(ESession) : 
   
#       def __init__(self) :  
#           super().__init__("einvoice","https://einvoice1.gst.gov.in","UserLogin.UserName","UserLogin.Password","UserLogin.Salt","CaptchaCode")
#           self.cookies.set("ewb_ld_cookie",value = "292419338.20480.0000" , domain = "ewaybillgst.gov.in")             
#           self._login =  ("https://einvoice1.gst.gov.in/Home/Login", self.form)
#           self._get_captcha = "https://einvoice1.gst.gov.in/get-captcha-image"
       
#       def is_logged_in(self) : 
#         res = self.get("https://einvoice1.gst.gov.in/Home/MainMenu") #check if logined correctly .
#         if "https://einvoice1.gst.gov.in/Home/MainMenu" not in res.url : #reload faileD
#               self.update("cookies",None)
#               return False
#         return True 
    
#       def upload(self,json_data) : 
#           if not self.is_logged_in() : return jsonify({ "err" : "login again."}) , 501 
#           bulk_home = self.get("https://einvoice1.gst.gov.in/Invoice/BulkUpload").text
#           files = { "JsonFile" : ("eway.json", StringIO(json_data) ,'application/json') }
#           form = extractForm(bulk_home)
    
#           upload_home = self.post("https://einvoice1.gst.gov.in/Invoice/BulkUpload" ,  files = files , data = form ).text
#           success_excel = pd.read_excel(self.download("https://einvoice1.gst.gov.in/Invoice/ExcelUploadedInvoiceDetails"))
#           failed_excel =  pd.read_excel(self.download("https://einvoice1.gst.gov.in/Invoice/FailedInvoiceDetails"))
#           failed_excel.to_excel("failed.xlsx")
#           data = {  "download" :  success_excel.to_csv(index = False) ,  "success" : len(success_excel.index) , 
#                     "failed" : len(failed_excel.index) , "failed_data" : failed_excel.to_csv(index=False) } 
#           return  jsonify(data) 

# class Eway(ESession) : 

#       def __init__(self) :  
#           super().__init__("eway","https://ewaybillgst.gov.in","txt_username","txt_password","HiddenField3","txtCaptcha")
#           self.cookies.set("ewb_ld_cookie",value = "292419338.20480.0000" , domain = "ewaybillgst.gov.in")         
#           self._login =  ("https://ewaybillgst.gov.in/login.aspx", self.form)
#           self._get_captcha = "https://ewaybillgst.gov.in/Captcha.aspx"
      
#       def get_captcha(self):
#           ewaybillTaxPayer = "p5k4foiqxa1kkaiyv4zawf0c"   
#           self.cookies.set("ewaybillTaxPayer",value = ewaybillTaxPayer, domain = "ewaybillgst.gov.in" , path = "/")
#           return super().get_captcha()

#       def website(self) : 
#             for i in range(30) : 
#               try :
#                   return self.get("https://ewaybillgst.gov.in/login.aspx",timeout = 3)
#               except :
#                  logging.debug("Retrying Eway website")
#                  continue
#             raise Exception("EwayBill Page Not loading")          
            
#       def is_logged_in(self) : 
#            res = self.get("https://ewaybillgst.gov.in/mainmenu.aspx") #check if logined correctly .
#            if res.url == "https://ewaybillgst.gov.in/login.aspx" : 
#                #with open("error_eway_login.html","w+") as f : f.write(res.text)
#                return False 
#            else : return True 
    
#       def upload(self,json_data) : 
#           if not self.is_logged_in() : return jsonify({ "err" : "login again."}) , 501 
#           bulk_home = self.get("https://ewaybillgst.gov.in/BillGeneration/BulkUploadEwayBill.aspx").text

#           files = { "ctl00$ContentPlaceHolder1$FileUploadControl" : ("eway.json", StringIO(json_data) ,'application/json')}
#           form = extractForm(bulk_home)
#           form["ctl00$lblContactNo"] = ""
#           try : del form["ctl00$ContentPlaceHolder1$btnGenerate"] , form["ctl00$ContentPlaceHolder1$FileUploadControl"]
#           except : pass 

#           upload_home = self.post("https://ewaybillgst.gov.in/BillGeneration/BulkUploadEwayBill.aspx" ,  files = files , data = form ).text
#           form = extractForm(upload_home)
          
#           generate_home = self.post("https://ewaybillgst.gov.in/BillGeneration/BulkUploadEwayBill.aspx" , data = form ).text 
#           soup = BeautifulSoup(generate_home, 'html.parser')
#           table = str(soup.find(id="ctl00_ContentPlaceHolder1_BulkEwayBills"))
#           try :
#               excel = pd.read_html(StringIO(table))[0]
#           except : 
#              if "alert('Json Schema" in upload_home :  #json schema is wrong 
#                  with open("error_eway.json","w+") as f :  f.write(json_data)
#                  logging.error("Json schema is wrong")
#                  return {"status" : False , "err" : "Json Schema is Wrong"}
#           try : err = parseEwayExcel(excel)
#           except Exception as e : 
#                 logging.error("Eway Parser failed")
#                 excel.to_excel("error_eway.xlsx")
#           data = { "download" : excel.to_csv(index=False) }
#           return data
