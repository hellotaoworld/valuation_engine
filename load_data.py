import database_connection
import os
import pandas as pd
import numpy as np
from datetime import datetime
import zipfile

# Helper functions
def mapfields(tag, table):
    for col in table.columns:
        if tag in table[col].values:
            return col
    return None

def transform_symbol(column_list):
  return ["`" + column + "`" for column in column_list]

def run(company,year):
    # Establish connection and cursor
    connection = database_connection.establish_local_database()
    cursor = connection.cursor()

    # tag mapping
    mapping_bs = pd.read_excel('./Valuation_Engine_Mapping.xlsx',sheet_name = "BS tags")
    mapping_is = pd.read_excel('./Valuation_Engine_Mapping.xlsx',sheet_name = "IS tags")
    mapping_cfo = pd.read_excel('./Valuation_Engine_Mapping.xlsx',sheet_name = "CFS tags")
    mapping_table = pd.concat([mapping_bs, mapping_is, mapping_cfo], axis=1)

    # Get Extract dir
    extractdir = r"\\DESKTOP-1OH4GP0\Users\Chris\Documents\GitHub\hellotaoworld\data_load\edgar_dataset\extract"

    # List of quarters
    extract_list = [item for item in os.listdir(extractdir)]

    # Get S&P 500 companies
    #company_query =f"SELECT * FROM valuation_engine_mapping_company"
    #df_sp500 = pd.read_sql(company_query, connection)

    # Get picked companies
    if company==['All']:
        company_query =f"SELECT cik FROM valuation_engine_mapping_company"
        params= None 
    else:
        placeholders = ', '.join(['%s'] * len(company))
        company_query =f"SELECT cik FROM valuation_engine_mapping_company where cik in ({placeholders})"
        params =tuple(company)
    df_sp500 = pd.read_sql(company_query, connection, params = params)
    #print(df_sp500)

        
    ###### Main Function #######
    def load_extract(qtr):
        
        # Get sub file
        subfile = extractdir+"/"+qtr+"/sub.txt"
        try:
            df_sub = pd.read_csv(subfile, sep='\t')
            #print(f"Loaded {subfile}")
            df_sub = df_sub[df_sub['form'].apply(lambda form: form=="10-K")]
            df_sp500sub = df_sub[df_sub['cik'].isin(df_sp500['cik'])]
        except Exception as e:
            print(f"Failed to read {subfile}: {e}", flush=True)

        # Get pre file
        prefile = extractdir+"/"+qtr+"/pre.txt"
        try:
            df_pre = pd.read_csv(prefile, sep='\t')
            # print(f"Loaded {prefile}")
            df_sp500pre = df_pre[df_pre['adsh'].isin(df_sp500sub['adsh'])]
            df_sp500pre = df_sp500pre[['adsh', 'report', 'stmt']]
            df_sp500pre = df_sp500pre[['adsh', 'report', 'stmt']].drop_duplicates()
        except Exception as e:
            print(f"Failed to read {prefile}: {e}", flush=True)
        
        # create adsh mapping
        adsh_sub = {row['adsh']: {'cik': row['cik'], 'fy': row['fy']} for _, row in df_sp500sub.iterrows()}
        
        # Load url table
        df_sp500pre['cik'] = df_sp500pre['adsh'].apply(lambda x: adsh_sub[x]['cik'] if x in adsh_sub else None)
        df_sp500pre['url'] = 'https://www.sec.gov/Archives/edgar/data/' + df_sp500pre['cik'].astype(str) + '/' + df_sp500pre['adsh'].str.replace('-', '').astype(str) + '/R' + df_sp500pre['report'].astype(str) + '.htm'
        df_sp500pre['fy']= df_sp500pre['adsh'].apply(lambda x: adsh_sub[x]['fy'] if x in adsh_sub else None)
        column_pre = transform_symbol(df_sp500pre.columns)
        for _, row in df_sp500pre.iterrows():
            insert_query = f"INSERT INTO valuation_engine_urls ({', '.join(column_pre)}) VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE adsh=VALUES(adsh), stmt=VALUES(stmt), url=VALUES(url)"
            values = tuple(row)
            cursor.execute(insert_query, values)
        
        # Get num file
        numfile = extractdir+"/"+qtr+"/num.txt"
        try:
            df_num = pd.read_csv(numfile, sep='\t')
            df_num = df_num.reset_index(drop=True)
            df_sp500num = df_num[df_num['adsh'].isin(df_sp500sub['adsh'])]
            df_sp500num = df_sp500num[df_sp500num['qtrs'].apply(lambda qtrs: qtrs==0 or qtrs==4)]
            df_sp500num = df_sp500num.loc[pd.isnull(df_sp500num.coreg),:]
            df_sp500num['mapping']=df_sp500num['tag'].apply(lambda tag: mapfields(tag, mapping_table))
            df_sp500num = df_sp500num[df_sp500num['mapping'].notna()]
            df_sp500num['cik'] = df_sp500num['adsh'].apply(lambda x: adsh_sub[x]['cik'] if x in adsh_sub else None)
            df_sp500num['fy'] = df_sp500num['adsh'].apply(lambda x: adsh_sub[x]['fy'] if x in adsh_sub else None)
            df_sp500num['updated_file']=qtr
            df_sp500num = df_sp500num.replace({np.nan: None})
            #print(df_sp500num)
        
            # Load dataset df_sp500num
            column_list = transform_symbol(df_sp500num.columns)
            for _, row in df_sp500num.iterrows():
                insert_query = f"INSERT INTO valuation_engine_inputs ({', '.join(column_list)}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE adsh=VALUES(adsh), coreg=VALUES(coreg), version=VALUES(version), qtrs=VALUES(qtrs), uom=VALUES(uom), value=VALUES(value), footnote=VALUES(footnote), mapping=VALUES(mapping), updated_file=VALUES(updated_file), fy=VALUES(fy)"
                values = tuple(row)
                cursor.execute(insert_query, values)
            
            #Commit the database change for a single file
            connection.commit()
            print(f"Data imported successfully from {qtr}.", flush=True)
        
        except Exception as e:
            #print(f"Exception: Failed to read {numfile}: {e}")
            print(f"no data extracted from {qtr} for selected companies.", flush=True)
            
    for qtr in extract_list:
        qtryear = qtr[:4]
        if year ==["All"]:
            load_extract(qtr)
        else:
            if qtryear in year:
                load_extract(qtr)    
    # load_extract('2017q3')

    # Close the cursor and connection
    cursor.close()
    connection.close()


### Enable for testing only ###
#company_selected = [1704720]
#year_selected=[2020]
#run(company_selected,year_selected)