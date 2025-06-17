from collections import Counter
from django.http import JsonResponse
import pandas as pd
from rest_framework.decorators import api_view
from app import models
from app.common import bulk_raw_insert
from custom.classes import IkeaDownloader

@api_view(["POST"])
def get_bill_products(request) : 
    bill_no = request.data.get("bill_no")
    i = IkeaDownloader()
    data = i.get(f"/rsunify/app/billing/retrievebill?billRef={bill_no}").json()
    salId = data["billHdVO"]["blhDsrId"]
    print(data)
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
    df["desc"] = df["desc"].str.strip()
    barcodes = models.Barcode.objects.filter(sku__in=df["sku"].values)
    barcodes = {b.sku: b.barcode for b in barcodes}
    df["barcode"] = df["sku"].map(lambda x: barcodes.get(x, None))
    cbu_maps = models.PurchaseProduct.objects.filter(sku__in = set(df["sku"].str.slice(0,5).values)).distinct()
    cbu_maps = {p.sku: p.cbu for p in cbu_maps}
    df["cbu"] = df["sku"].str.slice(0,5).map(lambda x: cbu_maps.get(x, None))
    df = df.sort_values(by=["mrp","sku"])
    return JsonResponse(df.to_dict(orient="records"),safe=False)


    