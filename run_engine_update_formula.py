import extract_data, load_data, calculate_metrics, calculate_ranking, send_email, load_formula_mapping
from datetime import datetime
import pytz
import sys
def gettime():
    est = pytz.timezone('US/Eastern')
    current_time = datetime.now(est)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S %Z')
    return formatted_time

start_time =gettime()
print(f"****** Valuation Engine is triggered {start_time}*****", flush=True)

# company should be in array list format, with the list of selected cik code, or "all"
# year should be in array list format, yyyy, or "all"
company =sys.argv[1].split(',')
print(company, flush=True)
year = sys.argv[2].split(',')
print(year, flush=True)

try:
    print(f'=== Update Formula Mapping {gettime()} ===', flush=True)
    load_formula_mapping.run()

    print(f'=== Start Calculating Metrics {gettime()} ===', flush=True)
    calculate_metrics.run(company,year)
    print(f'=== Metrics Calculation Completed ===', flush=True)

    print(f'=== Start Calculating Ranking {gettime()} ===', flush=True)
    calculate_ranking.run(company,year,'All')
    print(f'=== Ranking Calculation Completed ===', flush=True)
    
    end_time =gettime()
    print(f"****** Valuation Engine Finished Successfully {end_time} *****", flush=True)
    send_email.send_email(
            subject="Success: Python Script Completed",
            body=f"Hi, \n\nValuation Engine has finished executing successfully. \nVariable Input - Company: {company}; Year: {year}\nTime Start: {start_time} \nTime End: {end_time} \n\nEnjoy!",
            to_email="pienuuu@gmail.com"
        )
except Exception as e:
    print(f"An error occurred: {e}")
    send_email.send_email(
            subject="Fail: Python Script Error",
            body=f"Hi, \n\nValuation Engine has encountered errors. \nVariable Input - Company: {company}; Year: {year}\nTime Start: {start_time} \nTime End: {gettime()} \n\nError:\nAn error occurred: {e}",
            to_email="pienuuu@gmail.com"
        )
