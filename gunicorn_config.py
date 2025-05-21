bind = "0.0.0.0:8080"
workers = 1
threads = 5
max_requests = 1000
max_requests_jitter = 50

accesslog = "~/logs/access.log"
errorlog = "~/logs/error.log"
loglevel = "info"
