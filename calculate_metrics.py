import database_connection
import pandas as pd
import numpy as np
from datetime import datetime

# Helper functions
def transform_symbol(column_list):
  return ["`" + column + "`" for column in column_list]

# Assign Default Function
def ad(dataframe, index, field):
    if field in dataframe.columns:
        if pd.isna(dataframe.loc[index, field]):
            return 0
        else:
            return dataframe.loc[index,field]
    else:
        return 0


def run(company,year):
    # Establish connection and cursor
    connection = database_connection.establish_local_database()
    cursor = connection.cursor()

    # Read formula
    formula_query =f"SELECT * FROM valuation_engine_mapping_formula where formula_category <>'Custom Ratio'"
    mapping_formula_df = pd.read_sql(formula_query, connection)
    formula_names = mapping_formula_df['formula_shortname'].tolist()
    
    # Read company list (add criteria to be picked)
    if company==['All']:
        company_query =f"SELECT cik FROM valuation_engine_mapping_company"
        params= None 
    else:
        placeholders = ', '.join(['%s'] * len(company))
        company_query =f"SELECT cik FROM valuation_engine_mapping_company where cik in ({placeholders})"
        params =tuple(company)
    company_df = pd.read_sql(company_query, connection, params = params)
    ciklist = company_df['cik'].tolist()
    #company_names = company_df['company'].tolist()
    
    ### function ###
    def calculate_metrics(cik, year):
        if year ==["All"]:
            data_query =f"SELECT i.cik, left(i.ddate,4) as 'report_year', c.company as 'company_name', i.mapping, i.value FROM valuation_engine_inputs i left join valuation_engine_mapping_company c on i.cik=c.cik WHERE i.cik='{cik}'"
            params = None
        else:
            yr_placeholder = ', '.join(['%s'] * len(year))
            data_query =f"SELECT i.cik, left(i.ddate,4) as 'report_year', c.company as 'company_name', i.mapping, i.value FROM valuation_engine_inputs i left join valuation_engine_mapping_company c on i.cik=c.cik WHERE i.cik='{cik}' and i.fy IN ({yr_placeholder})"
            params = tuple(year)
        input_df = pd.read_sql(data_query, connection, params =params)
        #print(input_df)
        q_df = input_df.pivot_table(index=['cik', 'report_year', 'company_name'],columns='mapping', values='value',aggfunc='max').reset_index()
        if year !=["All"]:
            q_df = q_df[q_df['report_year'].isin(year)]
        #print(q_df)

        column_list = q_df.columns
        #print(column_list)
        # Load into database 
        # for _, row in q_df.iterrows():
        #     insert_query = f"INSERT INTO valuation_engine_values ({', '.join(transform_symbol(column_list))}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            
        #     values = tuple(row)
        #     cursor.execute(insert_query, values)

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
                ratio_df.loc[index, formula_name] = ratio_df.loc[index, formula_name].round(4)
        
        # Replace inf and -inf values with NULL
        ratio_df = ratio_df.replace([np.inf, -np.inf], np.nan)
        ratio_df=ratio_df.replace({np.nan: None})
        column_list = ratio_df.columns
        #print(ratio_df)
        #print(column_list)
        # Load into database 
        for _, row in ratio_df.iterrows():
            insert_query = f"INSERT INTO valuation_engine_metrics ({', '.join(transform_symbol(column_list))}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            values = tuple(row)
            cursor.execute(insert_query, values)
        connection.commit()
        print(f"Ratio updated successfully for {cik}.", flush=True)   
    
    # Refresh metric table for selected companies
    for cik in ciklist:
        if year ==["All"]:
            delete_query = f"DELETE FROM valuation_engine_metrics WHERE cik=%s"
            params =(cik, )
        else:
            placeholders = ', '.join(['%s'] * len(year))
            delete_query = f"DELETE FROM valuation_engine_metrics WHERE cik=%s and report_year in ({placeholders})"
            params = (cik,) + tuple(year)
        cursor.execute(delete_query, params=params)
        calculate_metrics(cik, year)        
    # calculate_metrics('34088')
    
    # Close the cursor and connection
    cursor.close()
    connection.close()

### Enable for testing only ###
#company_selected = [910638]
#year_selected=["All"]
#run(company_selected,year_selected)