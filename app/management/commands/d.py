import datetime 
import threading

from app.sync import sync_reports

def sync_beat_parties_ikea(force = True) :
    today = datetime.date.today() if not force else (datetime.date.today() + datetime.timedelta(days=1))
    #,"adjustment":today,"collection" : today,"beat": today,"party" : today,"beat" : today
    newly_synced = sync_reports(limits={"collection":today}) 
    


#create 5 simaulneous threads of sync_beat_parties_ikea()
threads = []
for _ in range(1):
    thread = threading.Thread(target=sync_beat_parties_ikea)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()



