import glob
import os
from custom.curl import save_request,get_curl
import sys

for file in glob.glob("custom/curl/**/*.txt", recursive=True) : 
    save_request(file)

os.system(f"git add *")
os.system(f"git commit -m '{sys.argv[2]}'")
os.system("git push")
exit(0)