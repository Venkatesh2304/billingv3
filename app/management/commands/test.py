from random import randint
from custom.classes import Billing
from concurrent.futures import ThreadPoolExecutor

# Initialize Billing instance

# Generate tasks
b = Billing()
r = randint(1, 10000)
tasks = [[f"A{r+i:05}" for i in range(10)] for i in range(500)]

def download_task(task):
    b.Download(task, pdf=True)
    
for _,task in enumerate(tasks) : 
    try: 
        download_task(task)
        print(_)
    except Exception as e:
        print(task)
        print(e)
        continue


print("All tasks completed!")