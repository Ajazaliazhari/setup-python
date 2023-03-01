import time,datetime
from datetime import date
import random
def stamp_generator():
    event_number = random.randint(1000000000,9999999999)
    event_date = date.today()
    now = datetime.datetime.now()
    event_time = now.strftime("%H:%M:%S")
    #print(event_number,event_date,event_time)
    #print(f"event no: {event_number} event_date: {event_date}  event_time {event_time}  ")
    return event_number,event_date,event_time
#stamp_generator()
