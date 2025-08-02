
from app.models import BarcodeMap
import json 
import datetime
from collections import defaultdict
from custom.classes import IkeaDownloader

maps = json.load(open("barcodes.json","r"))
BarcodeMap.objects.all().delete()
fromd = datetime.date(2025,4,1)
tod = datetime.date.today()

i = IkeaDownloader()
df = i.stock_master()
maps = dict(zip(df["Basepack Code"].values , df["Product Code"].values))

for k,v in maps.items() :
    BarcodeMap.objects.create(
        barcode = k,
        varient = v,
        sku = maps.get(v,None))