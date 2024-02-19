import database_connection
import pandas as pd
import numpy as np
from datetime import datetime

# Helper functions
def transform_symbol(column_list):
  return ["`" + column + "`" for column in column_list]

def run():
    # Establish connection and cursor
    connection = database_connection.establish_local_database()
    cursor = connection.cursor()

    # Read formula
    formula_query =f"SELECT * FROM valuation_engine_mapping_formula"
    mapping_formula_df = pd.read_sql(formula_query, connection)
    formula_names = mapping_formula_df['formula_shortname'].tolist()
    
    # Read company list
    company_query =f"SELECT * FROM valuation_engine_mapping_company where type='pick'"
    company_df = pd.read_sql(company_query, connection)
    ciklist = company_df['cik'].tolist()
    company_names = company_df['company'].tolist()
    
    ### function ###
    def calculate_metrics(cik):
        data_query =f"SELECT i.cik, left(i.ddate,4) as 'report_year', c.company as 'company_name', i.mapping, i.value FROM valuation_engine_inputs i left join valuation_engine_mapping_company c on i.cik=c.cik WHERE i.cik='{cik}' and i.fy =(SELECT max(fy) from web_application.valuation_engine_inputs where cik=i.cik and mapping =i.mapping and left(ddate,4)=left(i.ddate,4))"
        input_df = pd.read_sql(data_query, connection)
        
        q_df = input_df.pivot_table(index=['cik', 'report_year', 'company_name'],columns='mapping', values='value',aggfunc='sum').reset_index()
        column_list = q_df.columns
        #print(column_list)
        # Load into database 
        for _, row in q_df.iterrows():
            insert_query = f"INSERT INTO valuation_engine_values ({', '.join(transform_symbol(column_list))}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            values = tuple(row)
            #cursor.execute(insert_query, values)

        ratio_df = q_df[['cik', 'report_year', 'company_name']]
        #print(q_df['is.net_revenue'])
        
        # calculate
        for index, row in q_df.iterrows():
            for formula_name in formula_names:
                formula_value = mapping_formula_df.loc[mapping_formula_df['formula_shortname'] == formula_name, mapping_formula_df.columns[2]].values[0]
                try:
                    ratio_df.loc[index, formula_name] = eval(formula_value)
                except:
                    ratio_df.loc[index, formula_name] = 0
                ratio_df.loc[index, formula_name] = ratio_df.loc[index, formula_name].round(2)
        
        # Replace inf and -inf values with NULL
        ratio_df = ratio_df.replace([np.inf, -np.inf], np.nan)
        column_list = ratio_df.columns
        
        # Load into database 
        for _, row in ratio_df.iterrows():
            insert_query = f"INSERT INTO valuation_engine_metrics ({', '.join(transform_symbol(column_list))}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            values = tuple(row)
            cursor.execute(insert_query, values)
        connection.commit()
        print(f"Ratio updated successfully for {cik}.")   

    truncate_metric_query = f"TRUNCATE TABLE valuation_engine_metrics"
    cursor.execute(truncate_metric_query)
    print(f"Table valuation_engine_metrics is truncated.")
    
    for cik in ciklist:
        calculate_metrics(cik)        
    #calculate_metrics('2488')
    
    # Close the cursor and connection
    cursor.close()
    connection.close()

#run()