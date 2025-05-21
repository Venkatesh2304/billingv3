import os
import sys

os.system("rm -rf custom/")
os.system(f"cp -r /home/venkatesh/.local/lib/python3.10/site-packages/custom custom/")
os.system(f"git add *")
os.system(f"git status")
if input("check git status : ") != "y" : 
    os.system("rm -rf custom/")
    exit(0)
os.system(f"git commit -m '{sys.argv[2]}'")
os.system("git push")

if input("Delete custom directory :") == "n" : pass 
else : os.system("rm -rf custom/")

exit(0)