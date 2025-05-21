import datetime
from io import BytesIO
from app.common import *
from custom.classes import Billing,IkeaDownloader
from app.sales_import import CollectionInsert, PartyInsert
import app.models as models
from app.admin import sync_reports

# i = Billing()
# a = i.einvoice_json(datetime.date(2024,10,15),datetime.date.today(),["A49554"])  #A49554

# print(a)
from custom.classes import Eway
e= Eway()
with open("a.png","wb+") as f : f.write(e.captcha())
e.login(input(":"))


# sync_reports(limits={"sales" : 5*60,"collection" : None ,"adjustment" : None })
exit(0)
i = Billing()
df = i.loading_sheet([])
df.to_excel("a.xlsx",index=False)
df["MRP"] = df["MRP"].str.split(".").str[0]
df["LC"] = df["Total LC.Units"].str.split(".").str[0]
df["Units"] = df["Total LC.Units"].str.split(".").str[1]
df = df.rename(columns = {"Total FC" : "FC","Total Gross Sales" : "Gross Value"})
print( df ) #,"Pack Size"
df.to_html("a.html",columns=["Product Name","MRP","LC","Units","FC","UPC","Gross Value"],
           border=False,index=False,header=True,justify="left",na_rep="",col_space=30)
# with open("a.html")
print( df )


# i.crnote(fr)
#i.gstr_report(datetime.date.today(),datetime.date.today()).to_excel("a.xlsx")
# FRONT RS 10
sd
i.outstanding(datetime.date.today()).to_excel("o.xlsx")
dsf
i.collection(datetime.date(2024,9,25),datetime.date.today()).to_excel("a.xlsx")
i.outstanding(datetime.date.today()).to_excel("o.xlsx")


exit(0)
coll = i.donwload_manual_collection()
manual_coll = []
bill_chq_pairs = []
for bank_obj in models.Bank.objects.all() : 
    for coll_obj in bank_obj.collection.all() :
        bill_no  = coll_obj.bill_id 
        row = coll[coll["Bill No"] == bill_no].copy()
        row["Mode"] = ( "Cheque/DD"	if bank_obj.type == "cheque" else "Cheque/DD") ##Warning
        row["Retailer Bank Name"] = "KVB 650"	
        row["Chq/DD Date"]  = bank_obj.date.strftime("%d/%m/%Y")
        row["Chq/DD No"] = chq_no = f"{bank_obj.date.strftime('%d%m')}{bank_obj.idx}".lstrip('0')
        row["Amount"] = coll_obj.amt
        manual_coll.append(row)
        bill_chq_pairs.append((chq_no,bill_no))

manual_coll = pd.concat(manual_coll)
manual_coll["Collection Date"] = datetime.date.today()
f = BytesIO()
manual_coll.to_excel(f,index=False)
f.seek(0)
res = i.upload_manual_collection(f)


# print( pd.read_excel(i.download_file(res["ul"])).iloc[0] )


settle_coll = i.download_settle_cheque()
settle_coll = settle_coll[ settle_coll.apply(lambda row : (str(row["CHEQUE NO"]),row["BILL NO"]) in bill_chq_pairs ,axis=1)].iloc[:1]
settle_coll["STATUS"] = "SETTLED"
f = BytesIO()
settle_coll.to_excel(f,index=False)
f.seek(0)
res = i.upload_settle_cheque(f)
sync_ikea_report(i.collection, CollectionInsert,models.Collection,{})


print( pd.read_excel(i.download_file(res["ul"])) )
exit(0)


x = i.download_settle_cheque()
x = x.iloc[:1]
x["STATUS"] = "SETTLED"
# x["Collection Date"] = datetime.date.today()
# x["Mode"] = "Cheque/DD"	
# x["Retailer Bank Name"] = "KVB 650"	
# x["Chq/DD Date"] = "24/09/2024"	
# x["Chq/DD No"] = "5455"
# x["Amount"] = 2.0
print(x)
# sdf
b = BytesIO()
x.to_excel(b,index=False)
b.seek(0)

x = i.upload_settle_cheque(b)["ul"]
print( pd.read_excel(i.download_file(x)).iloc[0] )
exit(0)

# i = Billing(None,None,None,None,None)
# beats = i.get_plg_maps()
# bulk_raw_insert("beat",beats)
# PartyInsert(i.party_master())

for b in range(1,21) : 
    b = "A"+str(b).zfill(5)
    i.bills = [b]
    i.Download()
    os.system(f"mv bill.pdf bills/{b}.pdf")


exit(0)

query_db("delete from app_openingbalance")
opening_bal = i.outstanding(datetime(2024,3,31)).rename(columns={"Bill Number":"inum","Party Code":"party_id","O/S Amount":"amt","Beat Name":"beat"})
opening_bal = opening_bal.dropna(subset=["inum"])
opening_bal.amt = -opening_bal.amt
ledger_insert("openingbalance",opening_bal[["party_id","inum","amt","beat"]])
exit(0)

# from custom.classes import IkeaDownloader
# import datetime
# import pandas as pd 
# i = IkeaDownloader()

# fromd = datetime.date(2024,4,1)
# tod = datetime.date(2024,9,1)

# dates = pd.date_range(start=fromd,end=tod,freq='1D')
# no_of_days = 15 
# x  = []
# for idx in range(0,len(dates),no_of_days) : 
#     CollectionInsert( i.collection(dates[idx],dates[ min(idx+no_of_days,len(dates)-1) ]) )
#     print(idx,)