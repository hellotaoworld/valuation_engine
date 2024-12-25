import load_market, send_email, database_connection
from datetime import datetime
import pandas as pd
import pytz

def gettime():
    est = pytz.timezone('US/Eastern')
    current_time = datetime.now(est)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S %Z')
    return formatted_time

connection = database_connection.establish_local_database()
start_time =gettime()
print(f"****** PE Update is triggered {start_time}*****", flush=True)

load_market.run()
end_time =gettime()
update_query =f"select count(cik) as count from web_application.valuation_engine_inputs_market where mapping = 'pe_ratio' and updated_timestamp >=curdate()"
df_update = pd.read_sql(update_query, connection)
#print(df_update)
update_count = df_update['count'][0]
    
send_email.send_email(
    subject="Valuation Engine: PE Ratio Info Updated",
    body=f"Hi, \n\n {update_count} companies have updated PE Ratio today. \nTime Start: {start_time} \nTime End: {end_time} \n\nEnjoy!",
    to_email="pienuuu@gmail.com"
    )

print(f"****** PE Update Finished Successfully {end_time} *****", flush=True)