from collections import Counter, defaultdict
import datetime
import json
import sqlite3
from django.http import JsonResponse
import pandas as pd
from rest_framework.decorators import api_view
from app import models
from app.common import bulk_raw_insert, query_db
from custom.classes import IkeaDownloader

def get_sku_to_cbu_map(skus) : 
    import psycopg2
    rows = []
    for user in ["lakme_urban", "lakme_rural"]:
        conn = psycopg2.connect(
            dbname=user,           # replace with your DB name
            user="postgres",
            password="Ven2004",
            host="localhost",
            port="5432"
        )
        try:
            # Create a cursor
            cur = conn.cursor()
            sku_list = tuple(skus)
            query = """
                SELECT sku, cbu
                FROM app_purchaseproduct
                WHERE sku IN %s
            """
            cur.execute(query, (sku_list,))
            rows += cur.fetchall()
        finally:
            cur.close()
            conn.close()
    maps = list(set(rows))
    maps = {sku: cbu for sku, cbu in maps}
    return maps

@api_view(["POST"])
def get_closing_products(request) : 
    i1 = IkeaDownloader()
    i1.change_user("lakme_rural")
    df1 = i1.current_stock_with_mrp(datetime.date.today())
    df1 = df1[df1["Location"] == "MAIN GODOWN"]

    i2 = IkeaDownloader()
    i2.change_user("lakme_urban")
    df2 = i2.current_stock_with_mrp(datetime.date.today())
    df2 = df2[df2["Location"] == "MAIN GODOWN"]

    details = pd.concat([df1,df2],axis=0).drop_duplicates(subset=["SKU7","MRP"])[["SKU7","Product Name","MRP","Basepack Code","UPC"]]
    df1 = df1.groupby(["SKU7","MRP"])[["Units"]].sum().reset_index()
    df2 = df2.groupby(["SKU7","MRP"])[["Units"]].sum().reset_index()
    df = df1.merge(df2, on=["SKU7","MRP"], how="outer", suffixes=(" Rural"," Urban")).fillna(0)
    df["Total Qty"] = df["Units Rural"] + df["Units Urban"]
    df = df.merge(details, on=["SKU7","MRP"], how="left")
    df = df[["SKU7","Product Name","MRP","Total Qty","Basepack Code","UPC"]]
    df = df.groupby(["SKU7","Product Name","Basepack Code","UPC"]).agg({ "MRP" : "first" , "Total Qty" : "sum"}).reset_index()
    df = df.rename(columns={
        "SKU7": "sku",
        "Product Name": "name",
        "MRP": "mrp",
        "Total Qty": "total_qty",
        "Basepack Code": "itemvarient",
        "UPC": "upc"
    })
    df["itemvarient"] = df["itemvarient"].apply(lambda x : str(int(x)))

    maps = list(models.BarcodeMap.objects.filter(varient__in=df["itemvarient"].values).values_list("varient", "barcode"))
    varient_maps = defaultdict(list)
    for varient, barcode in maps:
        varient_maps[varient].append(barcode)
    
    maps = {varient: ",".join(barcodes) for varient, barcodes in varient_maps.items()}
    df["barcode"] = df["itemvarient"].apply(lambda x: maps.get(x, None))
    
    df["sku_small"] = df["sku"].str.slice(0,5)
    maps = get_sku_to_cbu_map(df["sku_small"].values)
    #models.PurchaseProduct.objects.filter(sku__in=df["sku_small"].values).values_list("sku", "cbu")            
    df["cbu"] = df["sku_small"].apply(lambda x: maps.get(x, None))

    df = df.sort_values(by=["mrp","sku"])
    return JsonResponse(df.to_dict(orient="records"),safe=False)
