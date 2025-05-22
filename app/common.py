from collections.abc import Iterable
import os
import sqlite3
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

PATH = "downloads/" #"/home/venkatesh/TALLY_DEVELOPMENT/ikea_sri_2022_2024/" #"downloads/"  
MODE = "pkl"

def read(f) -> pd.DataFrame | None :
    if MODE == "pkl" : 
        return pd.read_pickle(PATH + "/" + f + ".pkl")
    
def scalar_cols(df,dict) : 
    for k,v in dict.items() : df[k] = v 

def fetch_query(cur,query) : 
    cur.execute(query)
    return cur.fetchall()

### GENERAL 
def INVENTORY(df,type) :
    df = df.rename(columns = {"inum":f"{type}_id"}) 
    return df[ list(set(df.columns) & set(["stock_id","qty","txval","rt","bill_id","pur_bill_id","adj_bill_id","hsn","desc"])) ]

def both_insert( table , df1 , df2 , itype  , index ) :
    ledger_insert(table , df1, index )
    if df2 is not None : 
        inventory_insert(INVENTORY(df2,itype))
    
def ledger_insert( table , df , index ) : 
    if "party_id" in df.columns : 
        party = df[ list(set(df.columns) & set(["party_id","ctin"])) ]
        party = party.dropna(subset = ["party_id"])
        if "ctin" in party.columns:  
            party = party.sort_values("ctin")
            party = party[(~party[["ctin","party_id"]].duplicated()) | (df.ctin.isnull())]
        party["type"] = "supplier" if table in ["purchase"] else "shop"
        bulk_raw_insert("party",party.rename(columns={"party_id" : "code"}) ,is_partial_upsert=True,index="code")
    bulk_raw_insert(table,df,upsert=True,index=index) #,ignore=False

def inventory_insert( df ) : 
    if "hsn" in df.columns or "rt" in df.columns or "desc" in df.columns :
        stocks = df[ set(["stock_id","rt","hsn","desc"]) & set(df.columns) ].rename(columns={"stock_id":"name"}).drop_duplicates("name")
        if "desc" in stocks.columns : stocks["desc"] = stocks["desc"].str.strip().str.strip("\t")
        bulk_raw_insert("stock",stocks,is_partial_upsert=True,index="name")
    else : 
        stocks = df[['stock_id']].rename(columns={"stock_id":"name"})
        bulk_raw_insert("stock",stocks)

    if "hsn" in df.columns : del df["hsn"]
    if "desc" in df.columns : del df["desc"]

    scols = lambda cols : list( set(cols) & set(df.columns) )
    df = df.groupby( scols(("stock_id","bill_id","pur_bill_id","adj_bill_id")) ).aggregate( 
         {  col : agg for col,agg in { "txval" : "sum" , "qty" : "sum" , "rt" : "first" }.items() if col in set(df.columns) }
    ).reset_index()
    df["qty"] = df["qty"].abs()
    bulk_raw_insert("inventory",df)

def bulk_raw_insert(table,df,upsert=False,ignore=True,is_partial_upsert=False,index = None,partial_upsert_how="left",is_app_table=True) :
    if is_app_table : table = "app_" + table 
    if is_partial_upsert : 
       already = query_db(f"select * from {table.lower()}",is_select=True)
       diff_columns = list(set(already.columns) - set(df.columns)) + [index]
       df = df.merge( already[diff_columns] , how = partial_upsert_how , on = index )

    df = df.where(pd.notnull(df), None)
    rows = df.to_dict("tight")
    base_query = f"{table.lower()}({','.join(rows['columns'])}) values ({','.join( ['%s']*len(rows['columns']) )})"
    index = ",".join(index) if isinstance(index,list) else index
    if upsert or is_partial_upsert :
        print(table)
        query = f"INSERT into {base_query} ON CONFLICT({index}) DO UPDATE SET {', '.join([col + '=EXCLUDED.' + col for col in rows['columns']])}" 
    elif not ignore : 
        query = f"insert into {base_query}"
    else : 
        query = f"insert into {base_query} on CONFLICT({index}) DO NOTHING"
    query_db( query , many=True , values=rows["data"] )

def query_db(query,many=False,values=[],is_select=False) : 
    import os 
    from django.db import connection,transaction
 
    # connection = sqlite3.connect(f'db.sqlite3')
    # with transaction.atomic():  
    with connection.cursor() as cursor:
        if is_select : result = pd.read_sql(query,connection)
        elif many : cursor.executemany( query , values )
        else : cursor.execute( query )
        if is_select : return result 
        

update_rt_txval_query = lambda cond : f"""UPDATE app_inventory SET 
                              (rt,txval) = (SELECT rt,ROUND(txval*100/(100+2*rt),3) FROM app_stock WHERE name=stock_id)
                           WHERE {cond} and rt is NULL"""

calc_amt =  lambda table,itype,cond : f"""UPDATE app_{table} SET 
                                  amt = (SELECT -ROUND(SUM( txval*(100+2*rt) / 100 ),3) FROM app_inventory WHERE {itype}_id=inum)
                                  WHERE {cond}"""

calc_tds =  lambda table,itype,cond,tds_rate : f"""UPDATE app_{table} SET 
                                  (tds,amt) = (SELECT -ROUND(SUM(txval*{tds_rate}),3) , -ROUND(SUM( txval*(100+2*rt-{tds_rate*100}) / 100),3)  FROM app_inventory WHERE {itype}_id=inum)
                                  WHERE {cond}"""

calc_tcs =  lambda table,itype,cond,tcs_rate : f"""UPDATE app_{table} SET 
                                  (tcs,amt) = (SELECT ROUND(SUM( txval*(100+2*rt)*{tcs_rate}/100 ),3) , -ROUND(SUM( txval*(100+2*rt)*{1+tcs_rate}/100 ),3)  FROM app_inventory WHERE {itype}_id=inum)
                                  WHERE {cond}"""

moc_calc = """CASE
    WHEN strftime('%d', date) <= '20' THEN
      strftime('%m/%Y',date)
    ELSE
      strftime('%m/%Y', date, '+1 month', '-5 days')
    END"""

# calc_tds =  lambda table,itype,cond,ratio : f"""UPDATE app_{table} SET 
#                                   (tds,amt) = (SELECT -ROUND(SUM(txval)*{ratio},3) , ROUND(amt + SUM(txval)*{ratio},3)  FROM app_inventory WHERE {itype}_id=inum)
#                                   WHERE {cond} and tds = 0 """


# reverse_calc_amt_txval_query = lambda table,itype,cond : f"""UPDATE app_{table} SET 
#                               (amt,txval) = (SELECT -ROUND(SUM( txval  ),3), ROUND(SUM(txval*100/(100+2*rt)),3) FROM app_inventory WHERE {itype}_id=inum)
#                                WHERE {cond}"""