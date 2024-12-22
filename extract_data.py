import database_connection
import os
import pandas as pd
from datetime import datetime
import zipfile
#import yfinance as yf
import time 

def run(company,year):
    # Establish connection and cursor
    connection = database_connection.establish_local_database()
    cursor = connection.cursor()

    # Extract files if folder does not exist
    datasetdir = r"\\DESKTOP-1OH4GP0\Users\Chris\Documents\GitHub\hellotaoworld\data_load"
    rawdir = datasetdir+ "/edgar_dataset/rawdata"
    extractdir = datasetdir + "/edgar_dataset/extract"
    for filename in os.listdir(rawdir):
        if filename.endswith(".zip"):
            filepath = os.path.join(rawdir, filename)
            newpath = os.path.join(extractdir, filename[:-4]) 
            if not os.path.exists(newpath):
                os.makedirs(newpath)
                try:
                    with zipfile.ZipFile(filepath, 'r') as zip_ref:
                        zip_ref.extractall(newpath)
                        print(f"Extracted: {filename}", flush=True)
                except zipfile.BadZipFile:
                    print(f"Failed to extract {filename}: Corrupted ZIP file.", flush=True)
                except Exception as e:
                    print(f"Failed to extract {filename}: {e}", flush=True)

    # List of quarters
    extract_list = [item for item in os.listdir(extractdir)]
    #print(extract_list)
    #latest_folder = max(extract_list, key=lambda x: os.path.getmtime(os.path.join(extractdir, x)))
    
    def check_file(qtr):
        # Get latest sub file
        subfile = extractdir+"/"+qtr+"/sub.txt"
        df_sub = pd.read_csv(subfile, sep='\t')
        #print(df_sub)
        #print(f"Loaded {subfile}")
        df_sub = df_sub[df_sub['form']=='10-K']
        df_sub = df_sub[['sic','period','cik','fy']].drop_duplicates()
        df_sub['quarter'] = (df_sub['period'] // 100 % 100).apply(lambda x: (x - 1) // 3 + 1)
        df_sub['yyyyqx'] = df_sub.apply(lambda row: f"{row['fy'].astype(int)}q{row['quarter'].astype(int)}", axis=1)
        df_sub = df_sub[['sic','period','yyyyqx','cik']]
        #print(df_sub)
        
        # Get company selected
        if company==['All']:
            company_query =f"SELECT cik FROM valuation_engine_mapping_company"
            ticker_query=f"SELECT cik,symbol FROM valuation_engine_mapping_company"
            params= None 
        else:
            placeholders = ', '.join(['%s'] * len(company))
            company_query =f"SELECT cik FROM valuation_engine_mapping_company where cik in ({placeholders})"
            ticker_query=f"SELECT cik,symbol FROM valuation_engine_mapping_company where cik in ({placeholders})"
            params =tuple(company)
        
        df_sp500 = pd.read_sql(company_query, connection, params = params)
        df_sp500ticker = pd.read_sql(ticker_query, connection, params = params)

        df_sp500sub = df_sub[df_sub['cik'].isin(df_sp500['cik'])]
        df_sp500sub = df_sp500sub.merge(df_sp500ticker[['cik', 'symbol']], on= 'cik', how='left')
        
        #print(df_sp500sub)
        # Update company mapping on SIC code
        for _, row in df_sp500sub.iterrows():
            #### for exchange market field ####
            #exchange_info = yf.Ticker(row['symbol']).info.get("exchange", None)
            #update_sic = f"UPDATE valuation_engine_mapping_company SET sic=%s, fye=%s, qtr =%s, exchange=%s  WHERE cik=%s"
            #values = (row['sic'], row['period'], row['yyyyqx'], exchange_info, row['cik'])
            #print(values)
            update_sic = f"UPDATE valuation_engine_mapping_company SET sic=%s, fye=%s, qtr =%s WHERE cik=%s"
            values = (row['sic'], row['period'], row['yyyyqx'], row['cik'])
            cursor.execute(update_sic, values)
            update_industry = f"UPDATE valuation_engine_mapping_company c left join valuation_engine_mapping_sic s on c.sic=s.sic SET c.industry=s.industry"
            cursor.execute(update_industry)
    print(year)
    for qtr in extract_list:
        qtryear = qtr[:4]
        if year ==["All"]:
            check_file(qtr)
            print(f"Done extracting {qtr}.", flush=True) 
        else:
            if qtryear in year:
                check_file(qtr)
                print(f"Done extracting {qtr}.", flush=True) 
        
    
    
    # Close the cursor and connection
    cursor.close()
    connection.close()

### Enable for testing only ###
#company_selected = [910638]
#year_selected=["All"]
#run(company_selected,year_selected)