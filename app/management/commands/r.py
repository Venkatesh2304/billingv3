import time
import numpy as np
import pandas as pd
import json
import re
from datetime import datetime
from custom.classes import IkeaDownloader
from app.admin import sync_reports,query_db 
import os 
import app.models as models
import pandas as pd
from datetime import date 
from unittest.mock import patch
import unittest
from django.db.models import Count


import pandas as pd
import json

# # Load the Excel file
# excel_file = '~/Documents/LeverEDGE_41A392_E_Way_Bill_20241213072100210210.xlsx'
# df = pd.read_excel(excel_file)


class NpEncoder(json.JSONEncoder):
    """
    Custom JSON Encoder that converts non-serializable types like numpy int64, float64,
    pandas Timestamp, etc., to their Python native equivalents.
    """
    def default(self, obj):
        if isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32)):
            return round(float(obj),2)
        return super(NpEncoder, self).default(obj)
    
# def construct_json(df):
#     df = df.round(2)
#     # Group by Doc.No (Invoice Number)
#     grouped = df.groupby("Doc.No")

#     bill_lists = []

#     for doc_no, group in grouped:
#         # Extract a single row for non-item details
#         first_row = group.iloc[0]

#         # Process item details
#         item_list = []
#         for _, row in group.iterrows():
#             item = {
#                 "HsnCd": str(row["HSN"]),
#                 "SlNo": str(row.name),  # Row index as serial number
#                 "Unit": row["Units"],
#                 "Qty": row["Qty"],
#                 "IsServc": "N",
#                 "AssAmt": row["Assessable Value"],
#                 "UnitPrice": round(row["Assessable Value"] / row["Qty"],2) if row["Qty"] > 0 else 0,
#                 "TotItemVal": row["Total Amount"],
#                 "TotAmt": row["Assessable Value"],
#                 "GstRt": round(sum([ float(rt) for rt in row["Tax Rate"].split("+") ]),1) ,
#                 "CgstAmt": row["CGST Amount"],
#                 "SgstAmt": row["SGST Amount"]
#             }
#             item_list.append(item)

#         # Construct the JSON entry
#         bill = {
#             "Version": "1.1",
#             "TranDtls": {
#                 "TaxSch": "GST",
#                 "SupTyp": "B2B"  # Derived from "Supply Type"
#             },
#             "DocDtls": {
#                 "Typ": "INV" , #first_row["Doc type"]
#                 "No": doc_no,
#                 "Dt": first_row["Doc date"].strftime("%d/%m/%Y")
#             },
#             "SellerDtls": {
#                 "Gstin": first_row["From_GSTIN"],
#                 "LglNm": first_row["From Otherparty Name"],
#                 "Addr1": first_row["From_Address1"],
#                 "Loc": first_row['From_Place'],
#                 "Pin": first_row["From_pin_code"],
#                 "Stcd": "33"  
#             },
#             "BuyerDtls": {
#                 "Gstin": first_row["To_GSTIN"],
#                 "LglNm": first_row["To Otherparty Name"],
#                 "Pos": first_row["To_State"][:2],  # Assuming GST state codes are first 2 digits
#                 "Addr1": first_row["To_Address1"],
#                 "Loc": first_row["To_place"],
#                 "Pin": first_row["To_Pin_code"],
#                 "Stcd": "33"
#             },
#             "ValDtls": {
#                 "AssVal": group["Assessable Value"].sum(),  # Sum for group
#                 "CgstVal": group["CGST Amount"].sum(),      # Sum for group
#                 "SgstVal": group["SGST Amount"].sum(),      # Sum for group
#                 "OthChrg": first_row["TCS Amount"],         # From first row
#                 "TotInvVal": group["Total Amount"].sum()    # Sum for group
#             },
#             "ItemList": item_list
#         }
#         bill_lists.append(bill)

#     # Wrap in the main structure
#     result = {
#         "version": "1.0.0621",
#         "billLists": bill_lists
#     }
#     return result


# # Convert Excel data to JSON
# eway_json = construct_json(df)

# # Save the JSON to a file
# output_file = 'eway_output.json'
# with open(output_file, 'w') as f:
#     json.dump(eway_json, f,cls=NpEncoder, indent=4)

# print(f"JSON has been saved to {output_file}")
# asd

# # for date1 in pd.date_range(datetime(2024,12,1),datetime(2024,12,12),freq="1D") : 
# #     sync_reports(limits={"collection":None},today=date1.date(),min_days_to_sync={"collection":2})
# #     x = models.Collection.objects.filter(date__gte = date(2024,11,27)).values("date").annotate(count = Count("date")).values_list("date","count")
# #     print(list(x))
# #     sdf




# p = models.Sales.objects.all().count()
# for i in range(10) :
#     sync_reports(limits={"sales":None})
#     if p != models.Sales.objects.all().count() : 
#         print("New Collection Added")
#         break
#     print("No New Collection Added")
# sdfsdf
# ##Give 
# df1 = None 
# df2 = None 

# for i in range(1,13) : 
#     df2 = IkeaDownloader().collection(fromd=datetime(2024,12,5),tod=datetime(2024,12,12)).fillna("-")
#     del df2["Sr No"]
#     df2 = df2[df2["Collection Refr"] != "-"]
#     df2 = df2.sort_values("Collection Refr").reset_index(drop=True)
#     df2["Collection Date"] = pd.to_datetime(df2["Collection Date"],dayfirst=True).dt.date
#     if df1 is not None  : 
#          is_equal = df1.equals(df2) 
#          if not is_equal : 
#              print("Not Equal",i)
#              break 
#     df1 = df2 
#     print(i+1," Passed")

# if df1 is not None  : df1.to_excel("df1.xlsx")
# if df2 is not None  : df2.to_excel("df2.xlsx")


        


# # sync_reports({"collection" : None })

# # print( IkeaDownloader().download_settle_cheque("ALL",fromd=datetime(2024,12,1)) )
# sdf 


from custom.classes import Einvoice,Billing


bills = ["A62983"]

eway_data = Billing().eway_excel(bills)
eway_data['Doc date'] = eway_data['Doc date'].dt.strftime("%d/%m/%Y")
eway_data["CGST Rate"] = eway_data["Tax Rate"].str.split("+").str[0].astype(float)
eway_data["SGST Rate"] = eway_data["Tax Rate"].str.split("+").str[1].astype(float)
eway_data["To_Pin_code"] = eway_data["To_Pin_code"].fillna(620010)
eway_data["Distance level(Km)"] = 3
eway_data["Vehicle No"] = "TN49AF5764"

grouped = eway_data.groupby('Doc.No')

eway_json = {
    "version": "1.0.0621",
    "billLists": []
}

for doc_no, group in grouped:
    if doc_no is None:
        continue
    row = group.iloc[0]
    bill_json = {
        "userGstin": row['From_GSTIN'],
        "supplyType": "O",
        "subSupplyType": 1,
        "subSupplyDesc": "",
        "docType":  "INV", 
        "docNo": doc_no,
        "docDate": row['Doc date'],
        "transType": 1,
        
        "fromGstin": row['From_GSTIN'],
        "fromTrdName": row['From Otherparty Name'] ,
        "fromAddr1": row["From_Address1"],
        "fromAddr2": row["From_Address2"],
        "fromPlace": row["From_Place"] ,    
        "fromPincode": int(row['From_pin_code']),  
        "fromStateCode": 33 ,  
        "actualFromStateCode": 33 , 
        
        "toGstin": row['To_GSTIN'],
        "toTrdName": row['To Otherparty Name'] ,
        "toAddr1": row["To_Address1"],
        "toAddr2": row["To_Address2"],
        "toPincode": int(row['To_Pin_code']), 
        "toStateCode": 33 ,  
        "actualToStateCode": 33, 
        
        "totalValue": round(group['Assessable Value'].sum(),2),
        "cgstValue": round(group['CGST Amount'].sum(),2) ,
        "sgstValue": round(group['SGST Amount'].sum(),2),
        "OthValue": round(row['TCS Amount'],2),
        "totInvValue": round(row['Total Amount'],2),
        
        "transMode": 1,
        "transDistance": int(row['Distance level(Km)']),
        "transporterName": "",
        "transporterId": "",
        "transDocNo": doc_no,
        "transDocDate": row['Doc date'] ,
        "vehicleNo": row['Vehicle No'],
        "vehicleType": "R",
        "itemList": []
    }
    index = 1
    for _, item in group.iterrows():
        hsn = str(item['HSN']).split(".")[0]
        hsn = "0"*(8-len(hsn)) + hsn
        item_json = {
            "itemNo": index,
            "hsnCode": hsn ,
            "quantity": int(item['Qty']),
            "qtyUnit": "GMS",
            "taxableAmount": round(float(item['Assessable Value']),2),
            "cgstRate": round(item['CGST Rate'],1),
            "sgstRate": round(item['SGST Rate'],1),
            "igstRate": 0 ,
            "cessRate": 0 ,
        }
        bill_json['itemList'].append(item_json)
        index += 1 
    
    eway_json['billLists'].append(bill_json)

json_output_path = 'eway.json'
with open(json_output_path, 'w') as file:
    json.dump(eway_json, file,cls=NpEncoder, indent=4)

Einvoice().upload_eway_bill(json_output_path)