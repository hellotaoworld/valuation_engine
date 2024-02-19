import database_connection
import os
import pandas as pd
from datetime import datetime

### Update Cloud database ###

def transform_symbol(column_list):
  return ["`" + column + "`" for column in column_list]


def run():
  # Establish connection and cursor
  connection = database_connection.establish_local_database()
  connection_cloud = database_connection.establish_cloud_database()
  cursor_cloud = connection_cloud.cursor()
  
  # Load formula table
  mapping_formula_df = pd.read_sql(f"SELECT * FROM valuation_engine_mapping_formula", connection)
  formula_column_names = transform_symbol(mapping_formula_df.columns)
  for _, row in mapping_formula_df.iterrows():
        insert_query = f"INSERT INTO valuation_engine_mapping_formula ({', '.join(formula_column_names)}) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE formula_name=VALUES(formula_name),formula_value=VALUES(formula_value),formula_pseudo_code=VALUES(formula_pseudo_code),formula_category=VALUES(formula_category),formula_type=VALUES(formula_type),formula_direction=VALUES(formula_direction)"
        values = tuple(row)
        cursor_cloud.execute(insert_query, values)
  print("valuation_engine_mapping_formula is loaded.")
        
  # Load company table
  mapping_company_df = pd.read_sql(f"SELECT * FROM valuation_engine_mapping_company", connection)
  company_collist = transform_symbol(mapping_company_df.columns)
  for _, row in mapping_company_df.iterrows():
        insert_query = f"INSERT INTO valuation_engine_mapping_company ({', '.join(company_collist)}) VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE symbol=VALUES(symbol), company=VALUES(company), sic=VALUES(sic), industry=VALUES(industry), type=Values(type)"
        values = tuple(row)
        cursor_cloud.execute(insert_query, values) 
  print("valuation_engine_mapping_company is loaded.")
  
  # Load URL table
  url_query = f"SELECT * FROM web_application.valuation_engine_urls where stmt in ('BS','CF','IS') and fy >=2012 and cik in (SELECT cik from web_application.valuation_engine_mapping_company where `type`='pick');"
  url_df = pd.read_sql(url_query, connection)
  url_collist = transform_symbol(url_df.columns)
  for _, row in url_df.iterrows():
        insert_query = f"INSERT INTO valuation_engine_urls ({', '.join(url_collist)}) VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE adsh=VALUES(adsh), stmt=VALUES(stmt), url=VALUES(url)"
        values = tuple(row)
        cursor_cloud.execute(insert_query, values) 
  print("valuation_engine_urls is loaded.")
        
  # Load Ranking table
  ranking_query = f"SELECT * FROM web_application.valuation_engine_metrics_ranking;"
  ranking_df = pd.read_sql(ranking_query, connection)
  ranking_collist = transform_symbol(ranking_df.columns)
  for _, row in ranking_df.iterrows():
        insert_query = f"INSERT INTO valuation_engine_metrics_ranking ({', '.join(ranking_collist)}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE sic=VALUES(sic), industry=VALUES(industry), company_name=VALUES(company_name), metric_value=VALUES(metric_value), metric_ranking=VALUES(metric_ranking)"
        values = tuple(row)
        cursor_cloud.execute(insert_query, values)
  print("valuation_engine_metrics_ranking is loaded.")
  
  connection.commit()
  connection_cloud.commit()  
  # Close the cursor and connection
  cursor_cloud.close()
  connection.close()
  connection_cloud.close()
  

run()