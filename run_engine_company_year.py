import extract_data, load_data, calculate_metrics, calculate_ranking,update_cloud
import sys
print("****** Valuation Engine is triggered *****", flush=True)

# company should be in array list format, with the list of selected cik code, or "all"
# year should be in array list format, yyyy, or "all"
company =sys.argv[1].split(',')
print(company, flush=True)
year = sys.argv[2].split(',')
print(year, flush=True)

print('=== Extract data ===', flush=True)
extract_data.run(company,year)
print('=== Data extracted ===', flush=True)

print('=== Start Loading Data ===', flush=True)
load_data.run(company,year)
print('=== Data Load Completed ===', flush=True)

print('=== Start Calculating Metrics ===', flush=True)
calculate_metrics.run(company,year)
print('=== Metrics Calculation Completed ===', flush=True)

print('=== Start Calculating Ranking ===', flush=True)
calculate_ranking.run(company,year)
print('=== Ranking Calculation Completed ===', flush=True)
