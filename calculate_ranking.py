import database_connection
import pandas as pd
import numpy as np
from datetime import datetime

#### Calculate ranking and load to metric ranking table ###

def transform_symbol(column_list):
  return ["`" + column + "`" for column in column_list]


def run(company,year):
    # Establish connection and cursor
    connection = database_connection.establish_local_database()
    cursor = connection.cursor()

    # Table variables
    metric_list =  ['revenue_growth_rate',
                    'return_on_invested_capital_rate',
                    'eps_growth_rate', 
                    'adj_equity_growth_rate',
                    'free_cashflow_growth_rate',
                    'days_inventory_on_hand',
                    'days_sales_outstanding', 
                    'days_payable', 
                    'cash_conversion_cycle',
                    'working_capital_turnover', 
                    'total_asset_turnover', 
                    'quick_ratio',
                    'cash_ratio',
                    'debt_to_asset',
                    'debt_to_capital',
                    'debt_to_equity',
                    'financial_leverage',
                    'interest_ratio',
                    'gross_profit_margin',
                    'operating_profit_margin',
                    'ebitda_margin',
                    'net_profit_margin',
                    'return_on_asset',
                    'return_on_equity',
                    'sg_a_ratio',
                    'r_d_ratio', 
                    'depreciation_ratio', 
                    'cash_growth_rate',
                    'debt_growth_rate', 
                    'outstanding_shares_growth_rate',
                    'inventory_growth_rate',
                    'pp_e_growth_rate', 
                    'goodwill_growth_rate',
                    'total_asset_growth_rate']

    # Read metrics
    #metrics_query =f"SELECT * FROM {metric_table_name}"
    #mapping_formula_df = pd.read_sql(metrics_query, connection)
    #formula_names = mapping_formula_df.iloc[:, 4].tolist()
    
    # refresh industries of selected companies 
    if company==["All"]:
        industry_query =f"SELECT distinct industry FROM valuation_engine_mapping_sic"
        params= None 
    else:
        placeholders = ', '.join(['%s'] * len(company))
        industry_query =f"SELECT distinct s.industry FROM valuation_engine_mapping_company c left join valuation_engine_mapping_sic s on c.sic = s.sic where cik in ({placeholders})"
        params = tuple(company)
    
    industry_df = pd.read_sql(industry_query, connection, params = params)
    #print(industry_df)
    
    def calculate_ranking(metric_v, year):
        # refresh selected year only
        if year ==["All"]:
            year_query =f"SELECT distinct report_year as year FROM valuation_engine_metrics WHERE {metric_v} is not null order by report_year"
            params = None
        else:
            placeholders = ', '.join(['%s'] * len(year))
            year_query =f"SELECT distinct report_year as year FROM valuation_engine_metrics WHERE {metric_v} is not null and report_year in ({placeholders}) order by report_year"
            params = tuple(year)
        year_df = pd.read_sql(year_query, connection, params = params)
        #print(year_df)
        
        direction_query =f"SELECT formula_direction from valuation_engine_mapping_formula where formula_shortname='{metric_v}'"
        d_df = pd.read_sql(direction_query, connection)
        direction= d_df["formula_direction"][0]
        
        if direction=='positive':
            data_query =f"SELECT c.cik as cik, c.sic as sic, c.industry as industry, company_name, report_year,{metric_v} as metric_value  FROM valuation_engine_metrics m LEFT JOIN valuation_engine_mapping_company c on m.cik=c.cik WHERE  {metric_v} is not null order by report_year ASC, {metric_v} DESC"
        else:
            data_query =f"SELECT c.cik as cik, c.sic as sic, c.industry as industry, company_name, report_year,{metric_v} as metric_value  FROM valuation_engine_metrics m LEFT JOIN valuation_engine_mapping_company c on m.cik=c.cik WHERE  {metric_v} is not null order by report_year ASC, {metric_v} ASC"
            
        q_df = pd.read_sql(data_query, connection)
        ranking_df = q_df[['cik', 'sic', 'industry', 'company_name','report_year', 'metric_value']]
        ranking_df['metric_name']=metric_v
        
        # calculate ranking per sector, per year
        for industry in industry_df['industry']:
            for year in year_df['year']:
                temp_df =ranking_df[(ranking_df['report_year']==year) & (ranking_df['industry']==industry)]
                temp_df.reset_index()
                i = 1
                for index, row in temp_df.iterrows():
                    if i == 1:
                        ranking_df.loc[index,'metric_ranking'] = 1
                        i = i + 1
                    elif ranking_df["metric_value"][index-1] == ranking_df["metric_value"][index]:
                        ranking_df.loc[index,'metric_ranking'] = i-1
                        #print(f"year {year}")
                        #print(index)
                    else:
                        ranking_df.loc[index,'metric_ranking'] = i
                        i = i + 1
                    
        ranking_df = ranking_df[ranking_df['metric_ranking'].notna()]
        ranking_df = ranking_df.replace({np.nan: None})
        #print(ranking_df)
        ranking_table_columns = transform_symbol(ranking_df.columns)
        
        # Load into database 
        for _, row in ranking_df.iterrows():
            insert_query = f"INSERT INTO valuation_engine_metrics_ranking ({', '.join(ranking_table_columns)}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE sic=VALUES(sic), industry=VALUES(industry), company_name=VALUES(company_name), metric_value=VALUES(metric_value), metric_ranking=VALUES(metric_ranking)"
            values = tuple(row)
            #print(insert_query)
            #print(values)
            cursor.execute(insert_query, values)
        connection.commit()
        print(f"Ranking updated successfully for {metric_v}.", flush=True)  
        

    # Refresh the ratio table for the list of industries
    # for _, row in industry_df.iterrows():
    #     industry = row['industry']
        
    #     if year ==["All"]:
    #         delete_query = f"DELETE FROM valuation_engine_metrics_ranking WHERE industry=%s"
    #         params =(industry,)
    #     else:
    #         placeholders = ', '.join(['%s'] * len(year))
    #         delete_query = f"DELETE FROM valuation_engine_metrics_ranking WHERE industry=%s and report_year in ({placeholders})"
    #         params =(industry,) + tuple(year)
    #     #print(delete_query)
    #     cursor.execute(delete_query, params= params)
    # print(f"Table valuation_engine_metrics_ranking is cleared for seleted industries.", flush=True)

    # calculate ranking for the list of metrics
    for metric in metric_list:
        calculate_ranking(metric, year)


    # Close the cursor and connection
    cursor.close()
    connection.close()


### Enable for testing only ###
#company_selected = ['All']
#year_selected=["All"]
#run(company_selected,year_selected)