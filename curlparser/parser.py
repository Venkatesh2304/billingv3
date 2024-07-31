import argparse
import json
import shlex
from requests import Request,Session
from collections import OrderedDict, namedtuple
from urllib.parse import urlparse
import subprocess 
import json


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
    print( result )
    return json.loads(result.stdout)

# def parse(curl_command: str) -> Request:
#     cookies = OrderedDict()
#     header = OrderedDict()
#     body = None
#     method = "GET"

#     curl_command = curl_command.replace("\\\n", " ")

#     tokens = shlex.split(shlex.quote(curl_command))
#     parsed_args = parser.parse_args(tokens)

#     if parsed_args.command != "curl":
#         raise ValueError("Not a valid cURL command")

#     if not is_url(parsed_args.url):
#         raise ValueError("Not a valid URL for cURL command")

#     data = parsed_args.data.lstrip("$")
#     if data:
#         method = "POST"

#     if data:
#         try:
#             body = json.loads(data)
#         except json.JSONDecodeError:
#             header["Content-Type"] = "application/x-www-form-urlencoded"
#         else:
#             header["Content-Type"] = "application/json"

#     if parsed_args.request:
#         method = parsed_args.request

#     for arg in parsed_args.cookie:
#         try:
#             key, value = arg.split("=", 1)
#         except ValueError:
#             pass
#         else:
#             cookies[key] = value

#     for arg in parsed_args.header:
#         try:
#             key, value = arg.split(":", 1)
#         except ValueError:
#             pass
#         else:
#             header[key] = value

#     user = parsed_args.user
#     if user:
#         user = tuple(user.split(":"))
#     for k,v in header.items() :  header[k] = v.lstrip()
#     return OrderedDict(
#         method=method,
#         url=parsed_args.url,
#         auth=user,
#         cookies=cookies,
#         data=bytes(data, "utf-8").decode("unicode_escape") ,
#         json=body,
#         headers=header,
#     )
