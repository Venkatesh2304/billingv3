import datetime 
import threading

import pandas as pd

from app import models
from app.sync import sync_reports
from custom.classes import IkeaDownloader

def sync_beat_parties_ikea(force = True) :
    today = datetime.date.today() if not force else (datetime.date.today() + datetime.timedelta(days=1))
    #,"adjustment":today,"collection" : today,"beat": today,"party" : today,"beat" : today
    newly_synced = sync_reports(limits={"collection":today}) 


i = IkeaDownloader()
df = i.download_settle_cheque()
i.upload_settle_cheque
print("CHEQUE NO" in df.columns)
print(df)
      
# "CHEQUE NO"
salId
p = i.product_wise_purchase(datetime.date(2025, 4, 1),datetime.date(2025, 6, 19))
p.to_excel("product_wise_purchase.xlsx",index=False)
g = i.gstr_report(datetime.date(2025, 4, 1),datetime.date(2025, 6, 19))
g.to_excel("gstr_report.xlsx",index=False)
sdf


c = i.collection(datetime.date(2025, 6, 15),datetime.date(2025, 6, 17))
print(c.iloc[0])

# print(i.download_settle_cheque(type = "SETTLED"))
sdf

data = i.get(f"/rsunify/app/billing/retrievebill?billRef=CA00150").json()
salId = data["billHdVO"]["blhDsrId"]
i.get(f"/rsunify/app/billing/deletemutable?salesmanId={salId}")
df = pd.DataFrame(data["billingProductMasterVOList"])
df = df.rename(columns={
    "prodCode": "sku",
    "prodName" : "desc",
    "mrp": "mrp",
    "qCase": "cases",
    "qUnits": "units",
    "prodUpc": "upc"
})[["sku", "desc", "mrp", "cases", "units", "upc"]]
print(df)


sdf


df1,df2 = IkeaDownloader().loading_sheet(bills = ["CA00182"])
print(df1)
print(df2)
print(df1.iloc[0])
print(df2.iloc[0])
df1.to_excel("a.xlsx")

print(x)
x.to_excel("a.xlsx")

# sync_reports(limits={"party":None,"beat":None,"collection":None,"sales":None},min_days_to_sync={})
exit(0)
#create 5 simaulneous threads of sync_beat_parties_ikea()
threads = []
for _ in range(1):
    thread = threading.Thread(target=sync_beat_parties_ikea)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()



