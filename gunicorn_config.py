bind = "0.0.0.0:8080"
workers = 1
threads = 10
max_requests = 1000
max_requests_jitter = 50

accesslog = "/home/ubuntu/logs/access.log"
errorlog = "/home/ubuntu/logs/error.log"
loglevel = "info"
