from collections import defaultdict
from app.sync import sync_reports
sync_reports(limits={"party":None,"beat":None,"collection":None,"sales":None,"adjustment":None}
                ,min_days_to_sync=defaultdict(lambda : 15))

