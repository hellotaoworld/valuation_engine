import database_connection
import os
import pandas as pd
from datetime import datetime
import zipfile

def run():
    # Establish connection and cursor
    connection = database_connection.establish_local_database()
    cursor = connection.cursor()

    # Extract files if folder does not exist
    datasetdir = 'C:/Users/Chris/Documents/GitHub/hellotaoworld/data_load'
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
                        print(f"Extracted: {filename}")
                except zipfile.BadZipFile:
                    print(f"Failed to extract {filename}: Corrupted ZIP file.")
                except Exception as e:
                    print(f"Failed to extract {filename}: {e}")

    # List of quarters
    extract_list = [item for item in os.listdir(extractdir)]
    latest_folder = max(extract_list, key=lambda x: os.path.getmtime(os.path.join(extractdir, x)))

    # Get latest sub file
    subfile = extractdir+"/"+latest_folder+"/sub.txt"
    df_sub = pd.read_csv(subfile, sep='\t')
    #print(f"Loaded {subfile}")
    df_sub = df_sub[['sic','cik']].drop_duplicates()

    company_query =f"SELECT cik FROM valuation_engine_mapping_company"
    df_sp500 = pd.read_sql(company_query, connection)

    df_sp500sub = df_sub[df_sub['cik'].isin(df_sp500['cik'])]
    
    # Update company mapping on SIC code
    for _, row in df_sp500sub.iterrows():
        update_sic = f"UPDATE valuation_engine_mapping_company SET sic=%s WHERE cik=%s"
        values = tuple(row)
        cursor.execute(update_sic, values)
        update_industry = f"UPDATE valuation_engine_mapping_company c left join valuation_engine_mapping_sic s on c.sic=s.sic SET c.industry=s.industry"
        cursor.execute(update_industry)
    

    # Close the cursor and connection
    cursor.close()
    connection.close()

#run()