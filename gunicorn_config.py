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
user = os.environ.get('app_user')
config = CONFIG[user]
port = config['port']
bind = f"0.0.0.0:{port}"
workers = 1
threads = 10
max_requests = 1000
max_requests_jitter = 50

accesslog = f"/home/ubuntu/logs/access_{user}.log"
errorlog = f"/home/ubuntu/logs/error_{user}.log"
loglevel = "info"
