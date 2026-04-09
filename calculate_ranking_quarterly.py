import database_connection
import pandas as pd
import numpy as np
from datetime import datetime

#### Calculate quarterly ranking and load to metric ranking quarterly table ###

def transform_symbol(column_list):
  return ["`" + column + "`" for column in column_list]


def run(company, year, metrics):
    # Establish connection and cursor
    connection = database_connection.establish_local_database()
    cursor = connection.cursor()

    metric_list = ['revenue_growth_rate',
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

    # Get industries for selected companies
    if company == ["All"]:
        industry_query = f"SELECT distinct industry FROM valuation_engine_mapping_sic"
        params = None
    else:
        placeholders = ', '.join(['%s'] * len(company))
        industry_query = (
            f"SELECT distinct s.industry FROM valuation_engine_mapping_company c "
            f"left join valuation_engine_mapping_sic s on c.sic = s.sic "
            f"where cik in ({placeholders})"
        )
        params = tuple(company)
    industry_df = pd.read_sql(industry_query, connection, params=params)

    def calculate_ranking(metric_v, year):
        # Get distinct (year, quarter) combinations that have data for this metric
        if year == ["All"]:
            period_query = (
                f"SELECT distinct report_year as year, report_quarter as quarter "
                f"FROM valuation_engine_metrics_quarterly "
                f"WHERE {metric_v} is not null order by report_year, report_quarter"
            )
            params = None
        else:
            placeholders = ', '.join(['%s'] * len(year))
            period_query = (
                f"SELECT distinct report_year as year, report_quarter as quarter "
                f"FROM valuation_engine_metrics_quarterly "
                f"WHERE {metric_v} is not null and report_year in ({placeholders}) "
                f"order by report_year, report_quarter"
            )
            params = tuple(year)
        period_df = pd.read_sql(period_query, connection, params=params)

        direction_query = (
            f"SELECT formula_direction from valuation_engine_mapping_formula "
            f"where formula_shortname='{metric_v}'"
        )
        d_df = pd.read_sql(direction_query, connection)
        direction = d_df["formula_direction"][0]

        if direction == 'positive':
            data_query = (
                f"SELECT c.cik as cik, c.sic as sic, c.industry as industry, "
                f"c.company as company_name, q.report_year, q.report_quarter, "
                f"{metric_v} as metric_value "
                f"FROM valuation_engine_metrics_quarterly q "
                f"LEFT JOIN valuation_engine_mapping_company c on q.cik=c.cik "
                f"WHERE {metric_v} is not null "
                f"order by report_year ASC, report_quarter ASC, industry ASC, {metric_v} DESC"
            )
        else:
            data_query = (
                f"SELECT c.cik as cik, c.sic as sic, c.industry as industry, "
                f"c.company as company_name, q.report_year, q.report_quarter, "
                f"{metric_v} as metric_value "
                f"FROM valuation_engine_metrics_quarterly q "
                f"LEFT JOIN valuation_engine_mapping_company c on q.cik=c.cik "
                f"WHERE {metric_v} is not null "
                f"order by report_year ASC, report_quarter ASC, industry ASC, {metric_v} ASC"
            )

        q_df = pd.read_sql(data_query, connection)
        ranking_df = q_df[['cik', 'sic', 'industry', 'company_name', 'report_year', 'report_quarter', 'metric_value']].copy()
        ranking_df['metric_name'] = metric_v
        ranking_df['metric_ranking'] = np.nan

        # Calculate ranking per industry, per year, per quarter
        for industry in industry_df['industry']:
            for _, period_row in period_df.iterrows():
                yr = period_row['year']
                qtr = period_row['quarter']
                temp_df = ranking_df[
                    (ranking_df['report_year'] == yr) &
                    (ranking_df['report_quarter'] == qtr) &
                    (ranking_df['industry'] == industry)
                ]
                i = 1
                for index, row in temp_df.iterrows():
                    if i == 1:
                        ranking_df.loc[index, 'metric_ranking'] = 1
                        i = i + 1
                    elif ranking_df["metric_value"][index - 1] == ranking_df["metric_value"][index]:
                        ranking_df.loc[index, 'metric_ranking'] = i - 1
                    else:
                        ranking_df.loc[index, 'metric_ranking'] = i
                        i = i + 1

        ranking_df = ranking_df[ranking_df['metric_ranking'].notna()]
        ranking_df = ranking_df.replace({np.nan: None})
        ranking_table_columns = transform_symbol(ranking_df.columns)

        # Load into database
        for _, row in ranking_df.iterrows():
            insert_query = (
                f"INSERT INTO valuation_engine_metrics_ranking_quarterly "
                f"({', '.join(ranking_table_columns)}) "
                f"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                f"ON DUPLICATE KEY UPDATE sic=VALUES(sic), industry=VALUES(industry), "
                f"company_name=VALUES(company_name), metric_value=VALUES(metric_value), "
                f"metric_ranking=VALUES(metric_ranking)"
            )
            values = tuple(row)
            cursor.execute(insert_query, values)
        connection.commit()
        print(f"Quarterly ranking updated successfully for {metric_v}.", flush=True)

    # Calculate ranking for all metrics
    for metric in metric_list:
        calculate_ranking(metric, year)

    # Close the cursor and connection
    cursor.close()
    connection.close()


### Enable for testing only ###
# company_selected = ['796343']
# year_selected = ['2023']
# run(company_selected, year_selected, 'All')
