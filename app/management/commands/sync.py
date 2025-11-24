from collections import defaultdict
from app.sync import sync_reports
#,"beat":None,"collection":None,"sales":None,"adjustment":None
sync_reports(limits={"party":None}
                ,min_days_to_sync=defaultdict(lambda : 15))

