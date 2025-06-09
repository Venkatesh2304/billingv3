from collections import Counter
import contextlib
import datetime
from io import BytesIO
import secrets
import string
from django.http import FileResponse, JsonResponse
import pandas as pd
import pdfplumber
from rest_framework.decorators import api_view
from app.common import bulk_raw_insert
import app.models as models
from custom.classes import IkeaDownloader

def extract_product_quantities(bytesio):
    # 5 cm in points (1 inch = 72 points; 1 cm ≈ 28.35 points)
    width_limit_pts = 3.5 * 28.35  # ~141.75 pts
    qty_offset = 13.2 * 28.35
    codes = ""
    qtys = ""
    inum = None 
    with contextlib.redirect_stderr(None):  # Suppress all stdout
        with pdfplumber.open(bytesio) as pdf:
            for i, page in enumerate(pdf.pages):
                page = pdf.pages[i]
                if i == 0 :  
                    inum = page.extract_text().splitlines()[0].split(":")[-1].strip()
                cropped = page.within_bbox((0, 0, width_limit_pts, page.height))
                text = cropped.extract_text()
                codes += text + "\n"

                cropped = page.within_bbox(
                    (qty_offset, 0, qty_offset + width_limit_pts, page.height)
                )
                text = cropped.extract_text()
                qtys += text + "\n"

    codes = codes.split("SKU code")[-1].split("Net Payabl")[0].splitlines()[1:]
    cbu = codes[::2]
    sku = codes[1::2]

    qtys = qtys.split("Per Unit\n")[-1].splitlines()
    qtys = [qty.split("TAX")[0] for qty in qtys if qty]
    qtys = [qty for qty in qtys if qty]
    qtys = qtys[: len(codes)]
    mrps = [int(mrp.split(".")[0]) for mrp in qtys[1::2]]
    qtys = [int(qty.split("/")[0].strip()) for qty in qtys[::2]]
    return inum , list(zip(cbu, sku, mrps,qtys))

@api_view(["POST"])
def upload_purchase_invoice(request) :
    file = request.FILES.get("file") 
    bytesio = BytesIO(file.read())
    inum , product_quantities = extract_product_quantities(bytesio)
    df = pd.DataFrame(product_quantities, columns=["cbu", "sku", "mrp","qty"])
    df["inum_id"] = inum
    pur,_ = models.TruckPurchase.objects.get_or_create(inum=inum)
    models.PurchaseProduct.objects.filter(inum_id=inum).delete()
    bulk_raw_insert("purchaseproduct",df,ignore=False)
    return JsonResponse({"status": "success", "inum": inum , "loaded" : pur.load_id is not None })

@api_view(["GET","POST"])
def map_purchase_to_load(request) :
    load = models.TruckLoad.objects.filter(completed = False).last()
    if request.method == "GET":
        qs = models.TruckPurchase.objects.filter(load = load).values_list("inum", flat=True)
        return JsonResponse(list(qs), safe=False)
    else : 
        inums = request.data.get("inums")
        models.TruckPurchase.objects.filter(load=load).update(load_id=None)
        models.TruckPurchase.objects.filter(inum__in=inums).update(load=load)
        return JsonResponse({"status": "success"})

@api_view(["GET"])
def get_last_load(request) : 
    load = models.TruckLoad.objects.filter(completed = False).last()
    if load is None : 
        load = models.TruckLoad.objects.create(completed=False)
        load.save()
    return JsonResponse({ "id" : load.id , "completed" : load.completed }) 

@api_view(["POST"])
def finish_load(request) : 
    models.TruckLoad.objects.filter(completed = False).update(completed=True)
    return JsonResponse({"status": "success"})

@api_view(["GET"])
def get_cbu_codes(request) : 
    load = models.TruckLoad.objects.filter(completed = False).last()
    cbu_codes = models.PurchaseProduct.objects.filter(inum__load=load).values_list("cbu",flat=True).distinct()
    return JsonResponse(list(cbu_codes),safe=False)

@api_view(["POST"])
def barcode_to_cbu(request) : 
    barcode = request.data.get("barcode").upper().strip()
    obj = models.Barcode.objects.get(barcode=barcode)
    return JsonResponse({"cbu" : obj.cbu if obj else None })


@api_view(["POST"])
def map_barcode_to_cbu(request) : 
    barcode = request.data.get("barcode").upper().strip()
    cbu = request.data.get("cbu").strip().upper()
    models.Barcode.objects.create(barcode=barcode,cbu=cbu).save()
    return JsonResponse({"status": "success", "message": "Barcode mapped to CBU successfully"})


@api_view(["POST"])
def get_product(request) : 
    cbu = request.data.get("cbu").strip().upper()
    # cbu = barcode.split("(241)")[1].split("(10)")[0].strip().upper()
    load = models.TruckLoad.objects.filter(completed = False).last()
    products = models.PurchaseProduct.objects.filter(inum__load=load,cbu=cbu)
    mrp = models.PurchaseProduct.objects.filter(cbu = cbu).first().mrp
    purchase_qty = products.aggregate(total_qty=models.Sum("qty"))["total_qty"] or 0 
    already_scanned_qty = models    .TruckProduct.objects.filter(load=load,cbu=cbu).aggregate(total_qty=models.Sum("qty"))["total_qty"] or 0 
    return JsonResponse({
        "cbu": cbu ,
        "mrp": mrp ,
        "rem_qty": purchase_qty - already_scanned_qty
    })

@api_view(["POST"])
def scan_product(request) : 
    products = request.data.get("products")
    load = models.TruckLoad.objects.filter(completed=False).last()
    for product in products : 
        models.TruckProduct.objects.create(
            load=load,cbu=product["cbu"],qty=product["qty"]).save()
    return JsonResponse({"status": "success", "message": "Products scanned successfully"})
    # barcode = request.data.get("code").upper().strip().strip("\n")
    # scanned = request.data.get("scanned")
    # qty = request.data.get("qty",1)
    # cbu = None 
    # barcodes = []
    # load = models.TruckLoad.objects.filter(completed=False).last()
    
    # if scanned : 
    #     try : 
    #         cbu = barcode.split("(241)")[1].split("(10)")[0].strip().upper()
    #     except : 
    #         return JsonResponse({"status": "error", "message": "Invalid Barcode" , "status_code": "invalid_barcode"})
    #     barcodes.append(barcode)
    # else : 
    #     cbu = barcode.upper().strip()
    #     alphabet = string.ascii_lowercase + string.digits
    #     for i in range(qty) :
    #         barcodes.append( ''.join(secrets.choice(alphabet) for _ in range(20)) )
    
    # product,created = None,None
    # for barcode in barcodes : 
    #     product , created = models.TruckProduct.objects.get_or_create(
    #         barcode=barcode,
    #         defaults={"load_id" : load.id , "cbu": cbu}
    #     ) 

    # if created : 
    #     return JsonResponse({"status": "success", "message": "Product added", "cbu": cbu, 
    #                          "status_code": "product_added"})
    # else : 
    #     if product.load.id != load.id : 
    #         return JsonResponse({"status": "error", "message": "Product Scanned in Previous Load", "cbu": cbu, 
    #                          "status_code": "previous_load"})
    #     else : 
    #         return JsonResponse({"status": "error", "message": "Product Already Scanned", "cbu": cbu, 
    #                          "status_code": "product_already_scanned"})
        
@api_view(["GET"])
def load_summary(request) : 
    tod = datetime.date.today()
    fromd = tod - datetime.timedelta(days=15)
    product_master = IkeaDownloader().product_wise_purchase(fromd,tod)
    product_master["sku"] = product_master["Item Code"].str.slice(0,5)
    product_master["desc"] = product_master["Item Name"]
    # product_master.to_excel("product_master.xlsx", index=False)
    load = models.TruckLoad.objects.filter(completed = False).last()
    inums = list(load.purchases.values_list("inum",flat=True))
    products = models.PurchaseProduct.objects.filter(inum_id__in=inums).values_list("cbu","sku","qty","mrp")
    purchase_products = pd.DataFrame(products,columns=["cbu","sku","purchase_qty","mrp"])
    purchase_products1 = purchase_products.copy()
    purchase_products = purchase_products.groupby(["cbu","sku"]).sum().reset_index()
    load_cbu = list(models.TruckProduct.objects.filter(load=load).values("cbu","qty"))
    load_products = pd.DataFrame(load_cbu,columns=["cbu","qty"]).rename(columns={"qty":"load_qty"})
    load_products = load_products.groupby("cbu").sum().reset_index()
    #load_products = pd.DataFrame(Counter(load_cbu).items(),columns=["cbu","load_qty"])
    df = pd.merge(purchase_products, load_products, on="cbu", how="outer").fillna(0)
    df["diff"] = df["purchase_qty"] - df["load_qty"]
    df = pd.merge(df, product_master[["sku","desc"]].drop_duplicates(subset=["sku"]) , on="sku", how="left") 
    df = df[["cbu","desc","mrp","purchase_qty","load_qty","diff"]]
    bytesio = BytesIO()
    with pd.ExcelWriter(bytesio, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Summary')
        df[df["diff"] != 0].to_excel(writer, index=False, sheet_name='Mismatch')
        df[df["diff"] == 0].to_excel(writer, index=False, sheet_name='Correct')
        
    bytesio.seek(0)
    return FileResponse(
        bytesio,
        as_attachment=True,
        filename=f"load_summary_{load.id}.xlsx"
    )








        
        


    print(inums)
    sdfsd
    models.TruckLoad.objects.filter(completed = False).update(completed=True)
    return JsonResponse({"status": "success"})




