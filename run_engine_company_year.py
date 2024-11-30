import extract_data, load_data, calculate_metrics, calculate_ranking,update_cloud
import sys

# company should be in array list format, with the list of selected cik code, or "all"
# year should be in array list format, yyyy, or "all"
company =sys.argv[1].split(',')
#print(company)
year = sys.argv[2].split(',')
#print(year)

print('****** Valuation Engine is triggered *****')

print('=== Extract data ===')
extract_data.run(company,year)
print('=== Data extracted ===')

print('=== Start Loading Data ===')
load_data.run(company,year)
print('=== Data Load Completed ===')

print('=== Start Calculating Metrics ===')
calculate_metrics.run(company,year)
print('=== Metrics Calculation Completed ===')

print('=== Start Calculating Ranking ===')
calculate_ranking.run(company,year)
print('=== Ranking Calculation Completed ===')
