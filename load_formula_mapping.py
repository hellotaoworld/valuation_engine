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
  # Truncate formula table
  truncate_query = f"TRUNCATE TABLE valuation_engine_mapping_formula"
  cursor.execute(truncate_query)
  mapping_formula_df = pd.read_excel("./Valuation_Engine_Mapping.xlsx",sheet_name = "Formula")
  formula_column_names = ['formula_name', 'formula_value', 'formula_pseudo_code', 'formula_shortname', 'formula_category','formula_type','formula_direction']
  for _, row in mapping_formula_df.iterrows():
        insert_query = f"INSERT INTO valuation_engine_mapping_formula ({', '.join(formula_column_names)}) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE formula_name=VALUES(formula_name),formula_value=VALUES(formula_value),formula_pseudo_code=VALUES(formula_pseudo_code),formula_category=VALUES(formula_category),formula_type=VALUES(formula_type),formula_direction=VALUES(formula_direction)"
        values = tuple(row)
        cursor.execute(insert_query, values)
 
  connection.commit()  
  # Close the cursor and connection
  cursor.close()
  connection.close()
  

#run()