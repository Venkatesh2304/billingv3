from collections import defaultdict
import glob
import json
import re
from django.db import connection
import pandas as pd
from app.models import *
import warnings
warnings.filterwarnings("ignore")
from custom.curl import save_request,get_curl

for file in glob.glob("custom/curl/**/*.txt", recursive=True) : 
    save_request(file)

exit(0)





from custom.classes import IkeaDownloader

html = IkeaDownloader().get("https://leveredge18.hulcd.com/rsunify/app/rssmBeatPlgLink/loadRssmBeatPlgLink#!").text
salesman_ids = re.findall(r"<input type=\"hidden\" value=\"([0-9]+)\" />",html,re.DOTALL)[::3] 
salesman_names = pd.read_html(html)[0]["Salesperson Name"]
print( dict(zip(map(int,salesman_ids),salesman_names)) )



exit(0)


cur = connection.cursor()
# cur.execute("DELETE from app_creditlockbill")
print( pd.read_sql(f"SELECT * from app_billing",connection) )
print( pd.read_sql(f"SELECT * from app_processstatus",connection) )
print( pd.read_sql(f"SELECT * from app_creditlock",connection) )
print( pd.read_sql(f"SELECT * from app_orders",connection) )
print( pd.read_sql(f"SELECT * from app_orderproducts",connection) )
cur.execute("update app_orderproducts set allocated = 3 where id = 1")
exit(0)