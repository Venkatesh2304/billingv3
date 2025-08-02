from collections import Counter, defaultdict
from django.http import JsonResponse
import pandas as pd
from rest_framework.decorators import api_view
from app import models
from app.common import bulk_raw_insert
from custom.classes import IkeaDownloader
from app.stock_check import get_sku_to_cbu_map

@api_view(["POST"])
def get_bill_products(request) : 
    bill_no = request.data.get("bill_no").strip().upper()
    i = IkeaDownloader()
    data = i.get(f"/rsunify/app/billing/retrievebill?billRef={bill_no}").json()
    salId = data["billHdVO"]["blhDsrId"]
    i.get(f"/rsunify/app/billing/deletemutable?salesmanId={salId}")
    df = pd.DataFrame(data["billingProductMasterVOList"])
    df = df.rename(columns={
        "prodCode": "sku",
        "prodCCode" : "sku_small" , 
        "prodName" : "name",
        "mrp": "mrp",
        "qCase": "cases",
        "qUnits": "units",
        "prodUpc": "upc" ,
        "totalQtyUnits" : "total_qty",
        "itemVarCode" : "itemvarient"
    })[["sku", "sku_small","name", "mrp", "cases", "units", "upc" , "total_qty" , "itemvarient"]]
    df = df.groupby(["sku","sku_small","name", "mrp", "upc", "itemvarient"]).sum().reset_index()
    
    maps = list(models.BarcodeMap.objects.filter(varient__in=df["itemvarient"].values).values_list("varient", "barcode"))
    varient_maps = defaultdict(list)
    for varient, barcode in maps:
        varient_maps[varient].append(barcode)
    maps = {varient: ",".join(barcodes) for varient, barcodes in varient_maps.items()}
    df["barcode"] = df["itemvarient"].apply(lambda x: maps.get(x, None))
    del df["itemvarient"]
    #Deprecate : Use current rs purchase only 
    #maps = list(models.PurchaseProduct.objects.filter(sku__in=df["sku_small"].values).values_list("sku", "cbu"))
    #maps = {sku: cbu for sku, cbu in maps}
    #Use Purchases from both rural and urban 
    maps = get_sku_to_cbu_map(df["sku_small"].values)
    df["cbu"] = df["sku_small"].apply(lambda x: maps.get(x, None))
    df = df.sort_values(by=["mrp","sku"])
    return JsonResponse(df.to_dict(orient="records"),safe=False)

mapper = {}

@api_view(["POST"])
def get_product_from_barcode(request) : 
    barcode = request.data.get("barcode")
    if barcode in mapper : 
        return JsonResponse(mapper[barcode])
    
    if len(barcode) > 16 : 
        cbu = barcode.split("(241)")[1].split("(10)")[0].strip().upper()
        type = "cbu"
        product = models.PurchaseProduct.objects.filter(cbu=cbu).first()
        value = product.sku if product else None
    else : 
        import requests
        
        cookies1 = {
            'rack.session': 'BAh7CEkiD3Nlc3Npb25faWQGOgZFVG86HVJhY2s6OlNlc3Npb246OlNlc3Npb25JZAY6D0BwdWJsaWNfaWRJIkU4ZGJhMjJmMWQxYzllY2QxYmYyNThiNDFmMWFlYmRiODJkZTkzYzMwNDkyYWZjZjFhNzQ2MzAzM2I5ZjNjYTJjBjsARkkiCWNzcmYGOwBGSSIxQ0crYkhIOUYxZng2akptWFpTZjdSTjlTd3ArSVN1RVNvYll3MmRVaUx5az0GOwBGSSINdHJhY2tpbmcGOwBGewdJIhRIVFRQX1VTRVJfQUdFTlQGOwBUSSItN2U0YmQ3NTE0YTUwODYzMzlkODQ1MDUzMjcyZDAyMzIyNjQ1MTgxZgY7AEZJIhlIVFRQX0FDQ0VQVF9MQU5HVUFHRQY7AFRJIi1kYTM5YTNlZTVlNmI0YjBkMzI1NWJmZWY5NTYwMTg5MGFmZDgwNzA5BjsARg%3D%3D--cc8556856993753b28cef67408064a53b1141d9b',
        }

        headers1 = {
        'eventid': '202507261549434943',
        'role': '4',
        'gvalue': '1',
        'rscode': '41B862',
        'accesstoken': 'eyJhbGciOiJIUzI1NiJ9.eyJkYXRhIjp7ImVtcGNvZGUiOiI4MDAxMDI2NjAiLCJzYWxfY29kZSI6IjQxQjg2Ml9TTU4wMDAwMSIsInBhcnR5X2NvZGUiOiJQMTEiLCJiZWF0X3BsZyI6IkhVTDMiLCJiZWF0X2lkIjoiMjEiLCJyc2NvZGUiOiI0MUI4NjIiLCJwYXJjb2RlaHVsIjoiSFVMLTQxMzU3MVA5NjYifSwiZXhwIjoxNTk1MzgyODUxMjR9.7-bFyJGoqM9izWCX-e9W9QxW6XKtSfnQzHKipsajH8Q',
        'hulid': 'HUL-413571P966',
        'token': 'eyJhbGciOiJIUzI1NiJ9.eyJkYXRhIjoiODAwMTAyNjYwIiwiZXhwaXJlc0luIjoxNTk1MzgzNzE1MDIsInBvc2l0aW9uX2NvZGUiOiI0MUI4NjJfU01OMDAwMDEiLCJkZXZpY2VfaWQiOiI1NDA5YmRlOGFiNmNkN2NjIiwiYXR0X2ZsYWciOjB9.wCwAWqYzqSGHnsnuITiWiyp-uc6C7dbOXalmDvCsfmk',
        'xversioncode': '184',
        'isShikhar': '0',
        'salcode': '41B862_SMN00001',
        'empcode': '800102660',
        'versioncode': '84',
        'barcode': barcode,
        'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 15; motorola edge 50 pro Build/V1UMS35H.10-67-7-1)',
        'Connection': 'Keep-Alive',
        }

        cookies2 = {
            'rack.session': 'BAh7CEkiD3Nlc3Npb25faWQGOgZFVG86HVJhY2s6OlNlc3Npb246OlNlc3Npb25JZAY6D0BwdWJsaWNfaWRJIkVkZDRmMDQwNGUzNzA2YmI2ZjdiOWU4YjRkMGY4ODYwMTYyYWFkNmJjNTMwNGY4ZjhkZjZkMDAwM2ZhNTIwY2I1BjsARkkiCWNzcmYGOwBGSSIxMG9udG42YmcwV0d3YnBldDRnRDM4SUJLc01LZ0hmUTlqUE8wbGZTMlJUYz0GOwBGSSINdHJhY2tpbmcGOwBGewdJIhRIVFRQX1VTRVJfQUdFTlQGOwBUSSItN2U0YmQ3NTE0YTUwODYzMzlkODQ1MDUzMjcyZDAyMzIyNjQ1MTgxZgY7AEZJIhlIVFRQX0FDQ0VQVF9MQU5HVUFHRQY7AFRJIi1kYTM5YTNlZTVlNmI0YjBkMzI1NWJmZWY5NTYwMTg5MGFmZDgwNzA5BjsARg%3D%3D--d3e3a92943f10a6420199d0413a28c37ebe67ddd',
        }
        
        headers2 = {
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
        
        type = "itemvarient"
        value = None 

        for cookies,headers in [(cookies1,headers1),(cookies2,headers2)]:
            response = requests.get(
                'https://salesedgecdn-new.hulcd.com/salesedge/api/v1/products/get_bar_code_new_ui_v8_2',
                cookies=cookies,
                headers=headers,
            )
            s = response.json()
            if "productgroup" in s : 
                varients = [ j["itemvarient"] for i in s["productgroup"] for j in i["products"] ]
                value = varients[0]
                break
    mapper[barcode] = { "type" : type , "value" : value  }
    return JsonResponse({ "type" : type , "value" : value  })
    
