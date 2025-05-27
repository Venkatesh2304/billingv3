import os 
CONFIG = {
    "a1" : { 
        "port" : 8080 
    } , 
    "lakme_urban" : {
        "port" : 8081 
    } ,
    "lakme_rural" : {
        "port" : 8082 
    }
}
app_user = os.environ.get('app_user')
bind = f"0.0.0.0:{CONFIG[app_user]['port']}"
workers = 1
threads = 10
max_requests = 1000
max_requests_jitter = 50

accesslog = f"/home/ubuntu/logs/access_{app_user}.log"
errorlog = f"/home/ubuntu/logs/error_{app_user}.log"
loglevel = "info"
