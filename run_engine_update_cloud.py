import update_cloud, send_email
from datetime import datetime
import pytz

def gettime():
    est = pytz.timezone('US/Eastern')
    current_time = datetime.now(est)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S %Z')
    return formatted_time

start_time =gettime()
print(f"****** Cloud Update is triggered {start_time}*****", flush=True)

try:
    update_cloud.run()
    end_time =gettime()
    print(f"****** Cloud Update Finished Successfully {end_time} *****", flush=True)
    send_email.send_email(
            subject="Success: Python Script Completed",
            body=f"Hi, \n\nCloud update for Valuation Engine has finished executing successfully. \nTime Start: {start_time} \nTime End: {end_time} \n\nEnjoy!",
            to_email="pienuuu@gmail.com"
        )
except Exception as e:
    print(f"An error occurred: {e}")
    send_email.send_email(
            subject="Fail: Python Script Error",
            body=f"Hi, \n\nCloud update for Valuation Engine has encountered errors. \nTime Start: {start_time} \nTime End: {gettime()} \n\nError:\nAn error occurred: {e}",
            to_email="pienuuu@gmail.com"
        )
