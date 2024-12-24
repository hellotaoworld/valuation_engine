import yfinance as yf
import pandas as pd
import numpy as np
import database_connection
from datetime import datetime, timedelta
import warnings

# Suppress all warnings
warnings.filterwarnings("ignore")

def get_last_workday():
    today = datetime.now()
    if today.weekday() == 0:  # Monday
        return today - timedelta(days=3)
    elif today.weekday() == 6:  # Sunday
        return today - timedelta(days=2)
    elif today.weekday() == 5:  # Saturday
        return today - timedelta(days=1)
    else:
        return today - timedelta(days=1)
    
def transform_symbol(column_list):
  return ["`" + column + "`" for column in column_list]

def fetch_stock_prices(ticker, date):
    current_date = date
    stock =yf.Ticker(ticker)    
    info = stock.info
    pe_ratio = info['forwardPE']
    #dividend = info['dividendRate']
    #attempts = 0
    #while attempts < 3:
    try:
        formatted_date = current_date.strftime("%Y-%m-%d")
        #data = stock.history(start=(current_date - timedelta(days=1)).strftime("%Y-%m-%d"), end=formatted_date)
        #if not data.empty:
        return {
            'ddate': (current_date - timedelta(days=1)).strftime("%Y-%m-%d"),
            #'price': data["Close"].iloc[0],
            'pe_ratio':pe_ratio
            #'dividend':dividend
        }
        #current_date -= timedelta(days=1)
        #attempts += 1
    except Exception as e:
        # print(f"Error fetching price for {ticker} on {current_date.strftime('%Y-%m-%d')}: {e}")
        return None
    #return None

def calculate_ranking(year, metric_v):
    connection = database_connection.establish_local_database()
    cursor = connection.cursor()
    
    direction_query =f"SELECT formula_direction from valuation_engine_mapping_formula where formula_shortname='{metric_v}'"
    d_df = pd.read_sql(direction_query, connection)
    direction= d_df["formula_direction"][0]
    print(direction)
    industry_query =f"SELECT distinct industry FROM valuation_engine_mapping_sic"
    industry_df = pd.read_sql(industry_query, connection)
    print(industry_df)    
    if direction=='positive':
        data_query =f"SELECT c.cik as cik, c.sic as sic, c.industry as industry, c.company as company_name, {year} as report_year,m.value as metric_value  FROM valuation_engine_inputs_market m LEFT JOIN valuation_engine_mapping_company c on m.cik=c.cik WHERE  m.mapping = '{metric_v}' order by m.value DESC"
    else:
        data_query =f"SELECT c.cik as cik, c.sic as sic, c.industry as industry, c.company as company_name, {year} as report_year,m.value as metric_value  FROM valuation_engine_inputs_market m LEFT JOIN valuation_engine_mapping_company c on m.cik=c.cik WHERE  m.mapping = '{metric_v}' order by m.value ASC"
            
    q_df = pd.read_sql(data_query, connection)
    ranking_df = q_df[['cik', 'sic', 'industry', 'company_name','report_year', 'metric_value']]
    ranking_df['metric_name']=metric_v
    #print(ranking_df)
    
    # calculate ranking per sector, per year
    for industry in industry_df['industry']:
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

def run():
    connection = database_connection.establish_local_database()
    cursor = connection.cursor()
    ticker_query =f"with pe_ratio as (select cik from web_application.valuation_engine_inputs_market where mapping = 'pe_ratio' and updated_timestamp >=curdate()) SELECT c.cik, c.symbol FROM web_application.valuation_engine_mapping_company c left join pe_ratio m on c.cik = m.cik where m.cik is null"
    df_ticker = pd.read_sql(ticker_query, connection)
    #print(df_ticker)
    current_date = get_last_workday()
    current_year = current_date.year
    #print(current_date)
    
    ##### Fetch and load stock price #########    
    for _, row in df_ticker.iterrows():
        try:
            # Fetch stock price
            result = fetch_stock_prices(row['symbol'], current_date)
            #stock_price =float(f"{result['price']:.2f}") if result['price'] is not None else None
            stock_date= str(result['ddate']) if result['ddate'] is not None else None
            pe_ratio = float(f"{result['pe_ratio']:.2f}") if result['pe_ratio'] is not None else None
            #dividend = float(f"{result['dividend']:.2f}") if result['dividend'] is not None else None
            
            
            insert_query = f"""
            INSERT INTO valuation_engine_inputs_market 
            (cik, symbol, value, ddate, mapping)
            VALUES (%s, %s, %s, %s, %s) 
            ON DUPLICATE KEY UPDATE 
                value = VALUES(value), 
                mapping = VALUES(mapping), 
                ddate = VALUES(ddate)
            """
            #mapping = "stock_price"
            #values = (row['cik'], row['symbol'], stock_price, stock_date, mapping)
            #cursor.execute(insert_query, values)
            #connection.commit()
            
            mapping = "pe_ratio"
            values = (row['cik'], row['symbol'], pe_ratio, current_date, mapping)
            cursor.execute(insert_query, values)
            connection.commit()
            
            #mapping = "dividend"
            #values = (row['cik'], row['symbol'], dividend, current_date, mapping)
            #cursor.execute(insert_query, values)
            #connection.commit()
            
        except Exception as e:
            print(f"Error fetching market information for symbol {row['symbol']}: {e}")
    
    ####### Trigger Ranking Refresh for PE Ratio ##########
    calculate_ranking(current_year,'pe_ratio')

run()
