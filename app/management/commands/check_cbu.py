import datetime
import os
import subprocess
from custom.classes import IkeaDownloader

# Define the SSH key and remote server
ssh_key = "~/Downloads/new aws/LightsailDefaultKey-ap-south-1.pem"
remote_user = "ubuntu"
remote_host = "43.205.224.85"

# Step 1: SSH into remote and run export commands
export_cmd = (
    'PGPASSWORD=Ven2004 psql -U postgres -d lakme_rural -h localhost '
    '-c "\\COPY app_purchaseproduct TO \'app_purchaseproduct_rural.csv\' CSV HEADER" && '
    'PGPASSWORD=Ven2004 psql -U postgres -d lakme_urban -h localhost '
    '-c "\\COPY app_purchaseproduct TO \'app_purchaseproduct_urban.csv\' CSV HEADER"'
)

subprocess.run([
    "ssh",
    "-i", ssh_key,
    f"{remote_user}@{remote_host}",
    export_cmd
])

# Step 2: Copy both CSV files to local machine via SCP
subprocess.run([
    "scp",
    "-i", ssh_key,
    f"{remote_user}@{remote_host}:~/app_purchaseproduct_rural.csv",
    f"{remote_user}@{remote_host}:~/app_purchaseproduct_urban.csv",
    "."
])


#id,inum_id,cbu,sku,qty,mrp
# Step 3: Run the Python script to process the CSV files
import pandas as pd
df1 = pd.read_csv("app_purchaseproduct_rural.csv")
df2 = pd.read_csv("app_purchaseproduct_urban.csv")
# Combine the two DataFrames
skus = list(set(df1["sku"].values).union(set(df2["sku"].values)))
fromd = datetime.date(2025,4,1)
tod = datetime.date.today()
i1 = IkeaDownloader()
i1.change_user("lakme_urban")
df1 = i1.product_wise_purchase(fromd,tod)
i2 = IkeaDownloader()
i2.change_user("lakme_rural")
df2 = i2.product_wise_purchase(fromd,tod)

df1 = df1[~df1["Item Code"].str.slice(0,5).isin(skus)]
df2 = df2[~df2["Item Code"].str.slice(0,5).isin(skus)]
os.remove("app_purchaseproduct_rural.csv")
os.remove("app_purchaseproduct_urban.csv")
df1.to_excel("missing_cbu_urban.xlsx", index=False)
df2.to_excel("missing_cbu_rural.xlsx", index=False)
print("Missing CBU in Urban and Rural is saved in missing_cbu_urban.xlsx and missing_cbu_rural.xlsx respectively.")
print("Needed Purchase Invoice in Urban")
print(*list(set(df1["Supplier Invoice No"].dropna().apply(lambda x : str(x).split(".")[0]).values)),sep="\n")
print("Needed Purchase Invoice in Rural")
print(*list(set(df2["Supplier Invoice No"].dropna().apply(lambda x : str(x).split(".")[0]).values)),sep="\n")






