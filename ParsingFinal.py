from log import logw
from stamp_generator import stamp_generator
import requests as rq
import re
import time
import datetime
import subprocess
import dbcon as db
from config import indus_robo_campaign,info
st = time.time()
import sys,os

sdt = sys.argv
start_time = sdt[1]
end_time = sdt[2]
file_name = sdt[3]
##################################
# This below line extract the required data from log file acourding to start time and end time from file_name to new_file 
s = f"sed -n '/{start_time}/, /{end_time}/p' /tmp/{file_name} >/tmp/file_to_read.log"
subprocess.check_output("{}".format(s), shell=True, universal_newlines=True)

##In the below line , we deleted extra lines from the file and stored main_file
s3 = "sed '$d' /tmp/file_to_read.log >/tmp/file_to_read_main.log"
subprocess.check_output("{}".format(s3), shell=True, universal_newlines=True)
#################################
sg = stamp_generator()
log_str = f"Log Unique Id {sg[0]}  LogDate {sg[1]}  LogTime {sg[2]}"
f = open('/tmp/file_to_read_main.log','r')
if os.stat('/tmp/file_to_read_main.log').st_size!=0:
    a = f.read()

    ##### Data Parsing #####
    def split_on_empty_lines(s : str):
        # greedily match 2 or more new-lines
        blank_line_regex = r"(?:\r?\n){2,}"
        return re.split(blank_line_regex, s.strip())

    def split_data(d):
        a1 = d.split('\n')
        return a1

    def data_obj(data_element : list):
        # global log_str
        # global logw
        try:
            d = dict()
            time_value, event = data_element[0].split('Event:')
            time_value = time_value.strip()
            time_value = time_value.split('#')[0]
            time_value = time_value[0:-1]
            time_value = str(time_value)
            d['Datetime'] = time_value
            d['Event'] = event.strip()
            dnew_list = data_element[1:]
        except Exception as e:
            logw("info",f"ERROR   {log_str}")
        try:
            for row in dnew_list:
                try:
                    key, value = row.split(':')
                except:
                    temp = row.split(':')
                    key = ':'.join(temp[:-1])
                    value = temp[-1]
                d[key] = value.strip()
            return d
        except Exception as e:
            logw("info",f"ERROR   {log_str}")
    final_data = list()
    data_list = split_on_empty_lines(a)
    conn = ''
    conn = db.db_con(conn)
    for obj in data_list:
        split_obj = split_data(obj)
        data = data_obj(split_obj)
        final_data.append(data)
    event_name = input('\nEvent name required--> ')
    # event_name = "DialerFailure"
    for buff in final_data:
        try:
            if buff['Event'] == event_name:
                logw("info",f"{log_str}\nbuff_list  {buff}\n")
                try:
                    variables = buff['Chanvar']
                    variables = variables.split(',')    
                    dialer_variables = buff['DialerVal']
                    dialer_variables = dialer_variables.split(',')
                    custphno = variables[0]
                    lead_id = variables[1]
                    campaign_id = variables[2]
                    campaign_name = conn.select_query(f'select campaign_name from campaign','single')[0]
                    campaign_name = campaign_name if campaign_name!=None else ""
                    list_id = variables[3]
                    present_time = time.time()
                    present_time = str(present_time)
                    present_time = present_time.split('.')[0]   
                    session_id = present_time+lead_id
                    custUniqueid = dialer_variables[12]
                    call_disposition = ''
                    api_status = dialer_variables[14]
                except Exception as e:
                    logw("error",f"{log_str}        ERROR   {e}\n\n")
                try:
                    if campaign_id in  indus_robo_campaign['campaign_id']:
                        query = "select list_name from list "
                        retvar  = conn.select_query(query,'single')
                        if retvar!=None:
                            list_name = retvar[0]
                        else:
                            list_name = ''
                        tp_string = f"session_id={session_id} & mobile_no={custphno} & failure_time={present_time} & campaign_name={campaign_name} & list_name={list_name} & unique_id={custUniqueid} & call_status={call_disposition} &  event={'failure'} & campaign_id={campaign_id}"
                        tp_string = tp_string.encode()
                        logw("info",f"{log_str}         TP_STRING {tp_string}\n\n")
                        try:
                            #url =  f"https://{info['host']}{info['path']}?{tp_string}"
                            url =  f"https:{info['host']}{info['path']}?"
                            res = rq.get(url,params = tp_string,verify =False)
                            #pc = f"curl --insecure {url}" 
                            logw("info",f"{log_str}        RESPONSE:  {res}\n\n")
                            #response = subprocess.check_output("{}".format(pc), shell=True, universal_newlines=True)
                        except Exception as e:
                            logw("info",f"{log_str}\n      ERROR   {e}")
                except Exception as e:
                    logw("info",f"{log_str}\n        ERROR  {e}\n")
            else:
                logw("info",f"{log_str}      Not Such a {event_name} Event set on This Program")
        except Exception as e:
            logw("info",f"{log_str}      ERROR   {e}")    
        # else:
    # print(f'Currently no action set to on this event :-- {event_name}')
else:
    logw('info',f'\nThere is no data between the time duration you entered. Make sure there is data and the time entered is in format  as [ DD MM YY H:M:S ] for e.g --> 23 Nov 2022 17:25:25')
    logw('info',f"You entered wrong Dateformate which are-- start_time :  [{start_time}] and end_time : [{end_time}]  Please enter valid formate which is shown above")
    exit()
#print(f"Total execution time {time.time()-ts}")

