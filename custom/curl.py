import argparse
import json
import re
from requests import Request,Session
from collections import namedtuple
from urllib.parse import urlparse
import subprocess 
import json
from pathlib import Path
import os

def is_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

class CurlRequest(Request) : 
      def send(self,s:Session = None) :  
          if s is None : 
             print("Request sent via new created dummy session")
             return Session().send( self.prepare() )
          else : 
             self.cookies = s.cookies.get_dict()
             return s.send( self.prepare() )

def parse_file(fname) -> CurlRequest: 
    curl_code = subprocess.run([f"cat {fname} | curlconverter -"],capture_output=True,shell="bash")
    curl_code = curl_code.stdout.decode('ascii') 
    for x,y in [["response = requests.post(","_request = CurlRequest('POST',"],
                ["response = requests.get(","_request = CurlRequest('GET',"],
                ["response = requests.head(","_request = CurlRequest('HEAD',"]] : 
        curl_code = curl_code.replace(x,y)
    exec(curl_code)
    return locals()["_request"]    

def parse(command:str) : 
    result = subprocess.run(['curlconverter', '--language', 'json', '-'], text=True, input=command, capture_output=True)
    return json.loads(result.stdout)

def get_curl(key, base_path=None):
    if base_path is None:
        fname = f"{key}.txt"
        fldrs = [Path(os.getcwd()), Path(__file__).parent]
        for fldr in fldrs:
            temp_fname = (fldr / f"curl/{fname}").resolve()
            if os.path.exists(temp_fname):
                fname = temp_fname
                break
    else:
        fname = (base_path / f"{key}.txt").resolve()
    r = parse_file(fname)
    r.headers = {
        header: r.headers[header]
        for header in ["accept", "accept-language", "content-type", "User-Agent"]
        if header in r.headers
    }
    return r
    
def curl_replace(pat,replaces,str) : 
    pat = re.compile(pat)
    replace_pat = ""
    for i in range(pat.groups) : 
        replace_pat += (f"\{i+1}%%%" + replaces[i])
    return re.sub(pat,replace_pat,str).replace("%%%","") 
      
if __name__ == "__main__" : 
    ParsedCommand = namedtuple(
        "ParsedCommand",
        [
            "method",
            "url",
            "auth",
            "cookies",
            "data",
            "json",
            "header",
            "verify",
        ],
    )

    parser = argparse.ArgumentParser()

    parser.add_argument("command")
    parser.add_argument("url")
    parser.add_argument("-A", "--user-agent")
    parser.add_argument("-I", "--head")
    parser.add_argument("-H", "--header", action="append", default=[])
    parser.add_argument("-b", "--cookie", action="append", default=[])
    parser.add_argument("-d", "--data", "--data-ascii", "--data-binary", "--data-raw", default=None)
    parser.add_argument("-k", "--insecure", action="store_false")
    parser.add_argument("-u", "--user", default=())
    parser.add_argument("-X", "--request", default="")
