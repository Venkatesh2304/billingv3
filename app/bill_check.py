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
    i.get(f"/rsunify/app/billing/deletemutable?salesmanId={salId}")
    df = pd.DataFrame(data["billingProductMasterVOList"])
    df = df.rename(columns={
        "prodCode": "sku",
        "prodCCode" : "cbu" , 
        "prodName" : "name",
        "mrp": "mrp",
        "qCase": "cases",
        "qUnits": "units",
        "prodUpc": "upc" ,
        "totalQtyUnits" : "total_qty",
        "itemVarCode" : "itemvarient"
    })[["sku", "cbu","name", "mrp", "cases", "units", "upc" , "total_qty" , "itemvarient"]]
    df = df.groupby(["sku", "cbu", "name", "mrp", "cases", "units", "upc", "itemvarient"]).sum().reset_index()
    # df["desc"] = df["desc"].str.strip()
    # barcodes = models.Barcode.objects.filter(sku__in=df["sku"].values)
    # barcodes = {b.sku: b.barcode for b in barcodes}
    # df["barcode"] = df["sku"].map(lambda x: barcodes.get(x, None))
    # cbu_maps = models.PurchaseProduct.objects.filter(sku__in = set(df["sku"].str.slice(0,5).values)).distinct()
    # cbu_maps = {p.sku: p.cbu for p in cbu_maps}
    # df["cbu"] = df["sku"].str.slice(0,5).map(lambda x: cbu_maps.get(x, None))
    df = df.sort_values(by=["mrp","sku"])
    return JsonResponse(df.to_dict(orient="records"),safe=False)

@api_view(["POST"])
def get_product_from_barcode(request) : 
    barcode = request.data.get("barcode")
    if len(barcode) > 16 : 
        cbu = barcode.split("(241)")[1].split("(10)")[0].strip().upper()
        type = "cbu"
        value = models.PurchaseProduct.objects.filter(cbu=cbu).first().sku
    else : 
        import requests
        cookies = {
            'rack.session': 'BAh7CEkiD3Nlc3Npb25faWQGOgZFVG86HVJhY2s6OlNlc3Npb246OlNlc3Npb25JZAY6D0BwdWJsaWNfaWRJIkVkZDRmMDQwNGUzNzA2YmI2ZjdiOWU4YjRkMGY4ODYwMTYyYWFkNmJjNTMwNGY4ZjhkZjZkMDAwM2ZhNTIwY2I1BjsARkkiCWNzcmYGOwBGSSIxMG9udG42YmcwV0d3YnBldDRnRDM4SUJLc01LZ0hmUTlqUE8wbGZTMlJUYz0GOwBGSSINdHJhY2tpbmcGOwBGewdJIhRIVFRQX1VTRVJfQUdFTlQGOwBUSSItN2U0YmQ3NTE0YTUwODYzMzlkODQ1MDUzMjcyZDAyMzIyNjQ1MTgxZgY7AEZJIhlIVFRQX0FDQ0VQVF9MQU5HVUFHRQY7AFRJIi1kYTM5YTNlZTVlNmI0YjBkMzI1NWJmZWY5NTYwMTg5MGFmZDgwNzA5BjsARg%3D%3D--d3e3a92943f10a6420199d0413a28c37ebe67ddd',
        }
        headers = {
            'eventid': '20250712131955955',
            'role': '4',
            'gvalue': '1',
            'rscode': '41B864',
            'accesstoken': 'eyJhbGciOiJIUzI1NiJ9.eyJkYXRhIjp7ImVtcGNvZGUiOiI4MDAwNjU2NzkiLCJzYWxfY29kZSI6IjQxQjg2NF9TTU4wMDAwMiIsInBhcnR5X2NvZGUiOiJQNDAiLCJiZWF0X3BsZyI6IkhVTDMiLCJiZWF0X2lkIjoiMTQiLCJyc2NvZGUiOiI0MUI4NjQiLCJwYXJjb2RlaHVsIjoiSFVMLTQxMzU3MVAxNDg5In0sImV4cCI6MTU5NTM3MDY1NDQ5fQ.TchnlTDgg86fQvM1nj_IGgRJJLCqYxdfcVVjvrUZ5jY',
            'hulid': 'HUL-413571P1489',
            'token': 'eyJhbGciOiJIUzI1NiJ9.eyJkYXRhIjoiODAwMDY1Njc5IiwiZXhwaXJlc0luIjoxNTk1MzcwMjA4ODksInBvc2l0aW9uX2NvZGUiOiI0MUI4NjRfU01OMDAwMDIiLCJkZXZpY2VfaWQiOiI5MWMxYTYzMjllY2Q4YzA2IiwiYXR0X2ZsYWciOjB9.xKzaJLuYcAfNS-TBxR_dGWhwA3o_OQ9MI2TlTrDNGJ8',
            'xversioncode': '184',
            'isShikhar': '0',
            'salcode': '41B864_SMN00002',
            'empcode': '800065679',
            'versioncode': '84',
            'barcode': barcode,
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 15; motorola edge 50 pro Build/V1UMS35H.10-67-7-1)',
            'Connection': 'Keep-Alive',
        } #8901030978692
        response = requests.get(
            'https://salesedgecdn-new.hulcd.com/salesedge/api/v1/products/get_bar_code_new_ui_v8_2',
            cookies=cookies,
            headers=headers,
        )
        s = response.json()
        varients = [ j["itemvarient"] for i in s["productgroup"] for j in i["products"] ]
        type = "itemvarient"
        value = varients[0]
    return JsonResponse({ "type" : type , "value" : value  })
    
