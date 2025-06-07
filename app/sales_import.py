
from app.common import INVENTORY, bulk_raw_insert, inventory_insert, ledger_insert,query_db,both_insert,moc_calc,calc_tcs,calc_tds
import pandas as pd 
import app.models as models 


CLAIM_SERVICE_TDS_RATE = 0.02  

def delete_sales(type) : 
    query_db(f"DELETE FROM app_inventory WHERE bill_id in (select inum from app_sales where type='{type}')")
    query_db(f"DELETE FROM app_sales where type='{type}'")

##SPECIFIC 
def sales_insert(sales_reg,sales_gst,sales_type,permanent) : 
    sales_reg["type"] = sales_type
    sales_reg["txval"] = sales_reg[["crdsales","schdisc","cashdisc"]].sum(axis=1)
    sales_reg["discount"] = sales_reg[SALES_DISC_COLUMS[2:]].sum(axis=1)
    disc = pd.melt(sales_reg,id_vars=["inum"],value_vars=SALES_DISC_COLUMS,var_name='sub_type', value_name='amt_new')
    disc = disc.rename(columns={"amt_new":"amt"})
    disc["type"] = disc["sub_type"].replace({"schdisc":"stpr","pecom":"stpr"})
    disc = disc.rename(columns={"inum":"bill_id"})
    both_insert("sales",sales_reg[ models.Sales.columns + ["tcs"] ],sales_gst,"bill",index = "inum")
    print_table = sales_reg[["inum"]].rename(columns={"inum":"bill_id"})
    bulk_raw_insert("bill",print_table,ignore=True,index="bill_id")
    if permanent : bulk_raw_insert("discount",disc) #warning


# delete_sales('sales')
# delete_sales('salesreturn')
# delete_sales('claimservice')
# delete_sales('damage')
# delete_sales('shortage')
# query_db(f"DELETE FROM app_collection")
# query_db(f"DELETE FROM app_adjustment")

SALES_DISC_COLUMS = ["schdisc","cashdisc","pecom","btpr","outpyt","ushop","other discount"]

def refresh_outstanding(func) : 
    def decorated_function(*args,**kwargs) :
        func(*args,**kwargs)
        query_db('DELETE FROM app_outstanding')
        query_db('''
            INSERT INTO app_outstanding (party_id, inum, balance, beat, date)
            SELECT party_id, inum, SUM(amt) AS balance, MAX(beat) AS beat, MIN(date) AS date 
            FROM (
              SELECT party_id, inum, '2023-04-01' AS date, amt, beat FROM app_openingbalance
              UNION ALL
              SELECT party_id, inum, date, amt, beat FROM app_sales WHERE type = 'sales'
              UNION ALL
              SELECT party_id, bill_id AS inum, date, amt, NULL AS beat FROM app_collection
              UNION ALL
              SELECT party_id, to_bill_id AS inum, date, adj_amt AS amt, NULL AS beat FROM app_adjustment
            ) AS all_data
            GROUP BY party_id, inum 
        ''')
            # HAVING ABS(SUM(amt)) > 1
    return decorated_function

@refresh_outstanding
def SalesInsert(sales_reg,gst=None,permanent=False) : 
   
   if len(sales_reg.index) == 0 : return 

   ## Sales & Sales Return 
   sales_reg_map = { "BillRefNo":"inum","BillDate/Sales Return Date":"date", "Party Code" : "party_id" ,"SchDisc":"schdisc",
                     "CashDisc":"cashdisc","BTPR SchDisc":"btpr","OutPyt Adj":"outpyt","Ushop Redemption":"ushop" ,
                     "Adjustments":"pecom","GSTIN Number":"ctin","RoundOff":"roundoff","TCS Amt":"tcs","Beat":"beat"}
   
   sales_gst_map = { "Invoice No" : "inum", "Taxable" : "txval","UQC" : "stock_id","Total Quantity" : "qty" ,"Tax - Central Tax" : "rt","HSN":"hsn" , 
                    "HSN Description" : "desc" }

   df = sales_reg.rename(columns=sales_reg_map).iloc[:-1]
   df["CGST"] = df["SGST"] = (df["Tax Amt"] -  df["SRT Tax"])/2
   df["amt"] = -(df["BillValue"]+df["CR Adj"])
   df["other discount"] = df["DisFin Adj"] + df["Reversed Payouts"]
    
   df[SALES_DISC_COLUMS] = -df[SALES_DISC_COLUMS]
   df["crdsales"] = df["Crd Sales"] - df["Sal Ret"]
   
   sales_gst = None 
   if gst is not None : 
      gst = gst.rename(columns=sales_gst_map)
      ##Sales 
      sales_gst =  gst[gst.Transactions == "SECONDARY BILLING"]   
   
   sales_insert(df[df["crdsales"] > 0] , sales_gst ,"sales",permanent)
   
   
   if gst is None : return 
   ## Sales Return 
   sales_return_gst = gst[gst.Transactions == "SALES RETURN"]
   sales_return_reg = df[df["crdsales"] < 0]
   oi = "Original Invoice No"
   sales_return_reg = sales_return_reg.rename(columns={"inum":oi})
   sales_return_reg['cc'] = sales_return_reg.groupby(oi).cumcount()
   
   ## Important 
   sales_return_gst["Date"] = pd.to_datetime(sales_return_gst["Invoice Date"],format="%d/%m/%Y")
   inums_date_map = sales_return_gst.groupby([oi,"Invoice Date"],as_index=True)["Debit/Credit No"].min()
   sales_return_gst['inum']  = sales_return_gst.apply( lambda row : inums_date_map.loc[row[oi],row["Invoice Date"]] ,axis=1)
   sales_return_gst =  sales_return_gst.sort_values(by="Date")
   sales_return_gst['cc'] = sales_return_gst.drop_duplicates("inum").groupby(oi).cumcount()
   sales_return_reg = sales_return_reg.merge( sales_return_gst[[oi,"inum","cc"]] , on=[oi,"cc"] , how="left")
   
   sales_return_reg = sales_return_reg.rename(columns={ oi : "original_invoice_id" })
   sales_return_gst["txval"] = -sales_return_gst["txval"]
   sales_return_reg["roundoff"] = -sales_return_reg["roundoff"]
   sales_insert(sales_return_reg , sales_return_gst ,"salesreturn",permanent)
   query_db(f"UPDATE app_discount set moc = (select {moc_calc} from app_sales where inum=bill_id) where bill_id not like 'SI%'")
   
   
   additional_sales_gst_map = {"Invoice Date":"date","GSTIN of Recipient":"ctin","Invoice Value":"amt"}
   claims = gst[gst["Transactions"] == "CLAIMS SERVICE"].rename(columns=additional_sales_gst_map)
   claims = claims.groupby(["inum","date","ctin","stock_id","rt","hsn","desc","amt"]).aggregate({"txval":"sum","qty":"sum"}).reset_index()
   claims["type"] = "claimservice"
   claims["party_id"] = "HUL"
   claims["amt"] = -claims["amt"] 
   claims["date"] = pd.to_datetime(claims.date,dayfirst=True).dt.date
   
   inventory_insert( INVENTORY( claims , "bill" ) )
   ledger_insert( "sales" , claims.drop_duplicates(subset="inum")[["inum","date","party_id","type","ctin","amt"]] )
   query_db( calc_tds("sales","bill","type = 'claimservice'",CLAIM_SERVICE_TDS_RATE) )

@refresh_outstanding
def CollectionInsert(coll) : 
   coll_map = {"Collection Refr":"inum","Collection Date":"date","Coll. Amt" :"amt","Bill No":"bill_id"}
   coll = coll.rename(columns=coll_map)
   coll = coll.dropna(subset="inum")
   coll["date"] = pd.to_datetime(coll.date,dayfirst=True).dt.date
   coll = coll[coll.Status != "CAN"][coll.Status != "PND"]

   coll["bank_entry_id"] = None 
   is_auto_pushed_chq = (coll.Status == "CHQ") & (coll["Collection Settlement Mode"] == "Excel Collection")
   coll.loc[ is_auto_pushed_chq , "bank_entry_id" ] = coll.loc[ is_auto_pushed_chq , "Cheque No" ].astype(str).str.split(".").str[0]

   coll["mode"] = coll.Status.replace({"CSH":"Cash"}) # { k : "Bank" for k in ["CHQ","NEFT","RTGS","UPI","IMPS"] } 
   coll["party_id"] = None
   ledger_insert("collection",coll[ models.Collection.columns ],index = "inum")
   query = """UPDATE app_collection SET party_id = (
    select * from 
    (
        SELECT party_id FROM app_sales WHERE app_sales.inum = bill_id
        union all 
        SELECT party_id FROM app_openingbalance WHERE app_openingbalance.inum = bill_id
    ) as all_data
    limit 1)  WHERE party_id is NULL ;"""
   query_db( query )

@refresh_outstanding
def AdjustmentInsert(crnote) :  
    crnote_maps = {"CR/DR No.":"inum","Adjusted/Collected/Cancelled Date":"date","Adjusted Amt":"adj_amt",
                "Party Code":"party_id","Adjusted /Collected Bill No":"to_bill_id","Sales Ret Refr No.":"from_bill_id"}
    df = crnote.rename(columns=crnote_maps)
    df.loc[ df["Narration"] == "From Sales Return" , "from_bill_id" ] = df.loc[ df["Narration"] == "From Sales Return" , "inum" ]
    
    df["hash"] = pd.util.hash_pandas_object(df[["date","inum","adj_amt","from_bill_id","to_bill_id"]].astype(str), index=False)
    df['hash'] = df["hash"].apply(lambda x: format(x, 'x')[:5])
    df["inum"] = df["inum"] + "-" + df["hash"]
    df["date"] = df["date"].dt.date 
    df["amt"] = 0 #amt is always zero as there is no net transaction 
    ledger_insert("adjustment",df[models.Adjustment.columns],"inum")

def PartyInsert(party) : 
    pm = party.rename(columns={"Party Name":"name","Address":"addr","Party Code":"code",
                            "HUL Code":"hul_code","Party Master Code":"master_code"})[["addr","name","code","master_code","hul_code"]]
    pm = pm.drop_duplicates("code")
    strips = lambda df,val : df.str.split(val).str[0].str.strip(" \t,")
    pm["phone"] = pm["addr"].str.split("PH :").str[1].str.strip()
    pm["addr"] = strips( strips( strips( pm["addr"] , "TRICHY" )  , "PH :" ) , "N.A" )
    bulk_raw_insert("party",pm,is_partial_upsert=True,index="code")

def BeatInsert(beats) : 
    bulk_raw_insert("beat",beats,index=["id"])

# query_db("update app_inventory set qty = -abs(qty) where bill_id is not null and (select type from app_sales where inum=bill_id) in ('sales','claimservice') ")
# query_db("update app_inventory set qty = abs(qty) where bill_id is not null and (select type from app_sales where inum=bill_id) not in ('sales','claimservice') ")
