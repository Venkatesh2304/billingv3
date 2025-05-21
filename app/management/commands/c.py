from urllib.parse import urlencode
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
from datetime import datetime
from custom.classes import IkeaDownloader
from requests import Session
from custom.classes import extractForm 

def sync_impact(date,bills,vehicle_name):
    i = IkeaDownloader()
    login_data = i.post("/rsunify/app/impactDeliveryUrl").json()
    url = login_data["url"]
    del login_data["url"]
    url = url + "ikealogin.do?" + urlencode(login_data)
    s = Session() 
    s.get(url)
    s.get("https://shogunlite.com/")
    s.get("https://shogunlite.com/login.do") 
    html = s.get("https://shogunlite.com/deliveryupload_home.do?meth=viewscr_home_tripplan&hid_id=&dummy=").text 
    form = extractForm(html,all_forms=True)
    form =  {"org.apache.struts.taglib.html.TOKEN": form["org.apache.struts.taglib.html.TOKEN"],
            "actdate": date.strftime("%d-%m-%Y - %d-%m-%Y") , 
            "selectedspid": "493299",
            "meth":"ajxgetDetailsTrip"} #warning: spid is vehicle A1 (so we keep it default)
    html = s.get(f"https://shogunlite.com/deliveryupload_home.do",params=form).text 
    soup = BeautifulSoup(html,"html.parser")

    vehicle_codes = { option.text : option.get("value")  for option in soup.find("select",{"id":"mspid"}).find_all("option") }
    all_bill_codes = [ code.get("value") for code in soup.find_all("input",{"name":"selectedOutlets"}) ]
    all_bill_numbers = list(pd.read_html(html)[-1]["BillNo"].values)
    bill_to_code_map = dict(zip(all_bill_numbers,all_bill_codes))

    form = extractForm(html)
    form["mspid"] = vehicle_codes[vehicle_name]
    form["meth"] = "ajxgetMovieBillnumber"
    form["selectedspid"] = "493299"
    form["selectedOutlets"] = [ bill_to_code_map[bill] for bill in bills ] 
    del form["beat"]
    del form["sub"]
    s.post("https://shogunlite.com/deliveryupload_home.do",data = form).text


# Usage example
pdf_path = "bill.pdf"  # Path to the uploaded file
output_path = "filtered_pages.pdf"  # Path to save the new PDF
# extract_pages_with_large_blank_height(pdf_path, output_path, blank_threshold=640)

