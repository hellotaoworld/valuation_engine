import database_connection
import os
import pandas as pd
from datetime import datetime

### Update Mapping ###
def transform_symbol(column_list):
  return ["`" + column + "`" for column in column_list]


def run():
  # Establish connection and cursor
  connection = database_connection.establish_local_database()
  cursor = connection.cursor()

  # Load formula table
  mapping_formula_df = pd.read_excel("./Valuation_Engine_Mapping.xlsx",sheet_name = "Formula")
  formula_column_names = ['formula_name', 'formula_value', 'formula_pseudo_code', 'formula_shortname', 'formula_category','formula_type','formula_direction']
  for _, row in mapping_formula_df.iterrows():
        insert_query = f"INSERT INTO valuation_engine_mapping_formula ({', '.join(formula_column_names)}) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE formula_name=VALUES(formula_name),formula_value=VALUES(formula_value),formula_pseudo_code=VALUES(formula_pseudo_code),formula_category=VALUES(formula_category),formula_type=VALUES(formula_type),formula_direction=VALUES(formula_direction)"
        values = tuple(row)
        cursor.execute(insert_query, values)
        
  # Company Mapping
  # Truncate company mapping   
  truncate_query = f"TRUNCATE TABLE valuation_engine_mapping_company"
  cursor.execute(truncate_query)
  
  company_collist = ['`cik`','`symbol`','`company`']
  
  # Load S&P 500 company list to valuation_engine_mapping_company
  df_sp500 = pd.read_excel('./company_list.xlsx',sheet_name="sp500")
  df_sp500 = df_sp500[['cik','symbol','company']]
  #print(df_sp500)
  for _, row in df_sp500.iterrows():
        insert_query = f"INSERT INTO valuation_engine_mapping_company ({', '.join(company_collist)}) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE symbol=VALUES(symbol), company=VALUES(company)"
        values = tuple(row)
        cursor.execute(insert_query, values)
   
  # Load self-pick company list to valuation_engine_mapping_company
  df_pick = pd.read_excel('./company_list.xlsx',sheet_name="pick")
  for _, row in df_pick.iterrows():
        insert_query = f"INSERT INTO valuation_engine_mapping_company ({', '.join(company_collist)}) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE symbol=VALUES(symbol), company=VALUES(company)"
        values = tuple(row)
        cursor.execute(insert_query, values)
        update_query =f"UPDATE valuation_engine_mapping_company SET type='pick' WHERE cik=%s"
        cik = [values[0]]
        cursor.execute(update_query, cik)
  
  # Industry Mapping
  truncate_query = f"TRUNCATE TABLE valuation_engine_mapping_sic"
  cursor.execute(truncate_query)
  
  sic_collist = ['`sic`','`industry`']
  
  df_sic = pd.read_excel('./company_list.xlsx',sheet_name="sic")
  df_sic= df_sic[['sic','industry']]
  for _, row in df_sic.iterrows():
        insert_query = f"INSERT INTO valuation_engine_mapping_sic ({', '.join(sic_collist)}) VALUES (%s,%s) ON DUPLICATE KEY UPDATE industry=VALUES(industry)"
        values = tuple(row)
        cursor.execute(insert_query, values)
  
  
  connection.commit()  
  # Close the cursor and connection
  cursor.close()
  connection.close()
  

#run()