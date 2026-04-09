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
            return dataframe.loc[index, field]
    else:
        return 0

def get_quarter(ddate_val):
    month = int(str(int(ddate_val))[4:6])
    return f"Q{(month - 1) // 3 + 1}"


def run(company, year):
    # Establish connection and cursor
    connection = database_connection.establish_local_database()
    cursor = connection.cursor()

    # Read formula
    formula_query = f"SELECT * FROM valuation_engine_mapping_formula where formula_category <>'Custom Ratio'"
    mapping_formula_df = pd.read_sql(formula_query, connection)
    formula_names = mapping_formula_df['formula_shortname'].tolist()

    # Read company list
    if company == ['All']:
        company_query = f"SELECT cik FROM valuation_engine_mapping_company"
        params = None
    else:
        placeholders = ', '.join(['%s'] * len(company))
        company_query = f"SELECT cik FROM valuation_engine_mapping_company where cik in ({placeholders})"
        params = tuple(company)
    company_df = pd.read_sql(company_query, connection, params=params)
    ciklist = company_df['cik'].tolist()

    ### function ###
    def calculate_metrics_quarterly(cik, year):
        # Always fetch all years so growth rate formulas can reference prior year same quarter
        data_query = (
            f"SELECT i.cik, left(i.ddate,4) as 'report_year', i.ddate, "
            f"c.company as 'company_name', i.mapping, i.value "
            f"FROM valuation_engine_inputs i "
            f"left join valuation_engine_mapping_company c on i.cik=c.cik "
            f"WHERE i.cik='{cik}' AND i.form_type='10-Q'"
        )
        input_df = pd.read_sql(data_query, connection)
        if input_df.empty:
            return

        input_df['report_quarter'] = input_df['ddate'].apply(get_quarter)

        q_df = input_df.pivot_table(
            index=['cik', 'report_year', 'report_quarter', 'company_name'],
            columns='mapping',
            values='value',
            aggfunc='max'
        ).reset_index()

        # Filter to requested years after pivot (so prior-year rows are available during formula eval)
        if year != ["All"]:
            q_df_target = q_df[q_df['report_year'].isin(year)].copy()
        else:
            q_df_target = q_df.copy()

        if q_df_target.empty:
            return

        ratio_df = q_df_target[['cik', 'report_year', 'report_quarter', 'company_name']].copy()

        for index, row in q_df_target.iterrows():
            for formula_name in formula_names:
                formula_value = mapping_formula_df.loc[
                    mapping_formula_df['formula_shortname'] == formula_name,
                    mapping_formula_df.columns[2]
                ].values[0]
                try:
                    ratio_df.loc[index, formula_name] = eval(formula_value)
                except:
                    ratio_df.loc[index, formula_name] = 0
                ratio_df.loc[index, formula_name] = ratio_df.loc[index, formula_name].round(4)

        # Replace inf and -inf values with NULL
        ratio_df = ratio_df.replace([np.inf, -np.inf], np.nan)
        ratio_df = ratio_df.replace({np.nan: None})

        column_list = ratio_df.columns
        value_placeholders = ', '.join(['%s'] * len(column_list))
        for _, row in ratio_df.iterrows():
            insert_query = (
                f"INSERT INTO valuation_engine_metrics_quarterly ({', '.join(transform_symbol(column_list))}) "
                f"VALUES ({value_placeholders}) "
                f"ON DUPLICATE KEY UPDATE "
                + ', '.join([f"{c}=VALUES({c})" for c in transform_symbol(column_list) if c not in ['`cik`', '`report_year`', '`report_quarter`']])
            )
            values = tuple(row)
            cursor.execute(insert_query, values)
        connection.commit()
        print(f"Quarterly metrics updated successfully for {cik}.", flush=True)

    # Refresh quarterly metric table for selected companies
    for cik in ciklist:
        if year == ["All"]:
            delete_query = f"DELETE FROM valuation_engine_metrics_quarterly WHERE cik=%s"
            params = (cik,)
        else:
            placeholders = ', '.join(['%s'] * len(year))
            delete_query = f"DELETE FROM valuation_engine_metrics_quarterly WHERE cik=%s and report_year in ({placeholders})"
            params = (cik,) + tuple(year)
        cursor.execute(delete_query, params=params)
        calculate_metrics_quarterly(cik, year)

    # Close the cursor and connection
    cursor.close()
    connection.close()


### Enable for testing only ###
# company_selected = [796343]
# year_selected = ['2023']
# run(company_selected, year_selected)
