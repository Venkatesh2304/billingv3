
from app.models import BarcodeMap
import json 
import datetime
from collections import defaultdict
from custom.classes import IkeaDownloader

barcode_to_basepack = json.load(open("barcodes.json","r"))
BarcodeMap.objects.all().delete()
fromd = datetime.date(2025,4,1)
tod = datetime.date.today()

i = IkeaDownloader()
df = i.stock_master()
basepack_to_sku = dict(zip( df["Basepack Code"].apply(str).values , df["Product Code"].values))

for barcode,basepack in barcode_to_basepack.items() :
    BarcodeMap.objects.create(
        barcode = barcode,
        varient = basepack,
        sku = basepack_to_sku.get(basepack,None))