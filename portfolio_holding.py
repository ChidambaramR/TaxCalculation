import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import create_engine
import json

from gains import calculate_gains
from symbols_fetcher import fetch_symbols

# Function to load data from a JSON file
def load_data_from_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def get_symbol_details():
    file_path = 'stock_codes.db'
    database_url = f'sqlite:///{file_path}'
    engine = create_engine(database_url)
    sql_table_name = "codes"
    sql_query = f"SELECT code, name, isin FROM {sql_table_name}"
    sql_df = pd.read_sql_query(sql_query, con=engine)
    return sql_df
    
# Load data from JSON file
file_path = 'market_transactions.json'  # Update this to the path of your JSON file
data = load_data_from_json(file_path)

# Convert JSON data to DataFrame
df = pd.DataFrame(data)

# Convert transaction_date to datetime
df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='%d/%m/%Y')

# User-defined start date
fy_start_date = datetime.strptime("01/04/2023", "%d/%m/%Y")
fy_end_date = datetime.strptime("31/03/2024", "%d/%m/%Y")

# Function to filter transactions by date
def filter_by_date(df, end_date):
    return df[df['transaction_date'] <= end_date]

# Get transactions up to the start date
# filtered_df = filter_by_date(df, start_date)
df.rename(columns={"scrip_code": "code", "scrip_name": "name", "transaction_date": "ts"}, inplace=True)

fetch_symbols(df['code'])
symbols_df = get_symbol_details()
symbols_df.loc[len(symbols_df)] = {'code': "208539", 'name': 'NIPPONAMC', 'isin': 'NIPPONAMC'}

symbols_to_skip = ["2.50GOLDBONDS20", "800318", "GOLD", "NIPPON LIFE IND", 
                   "TOC BSE EXCHANG", "TOC NSE EXCHANG", "STT", "STAMP DUTY", 
                   "SERVICE TAX", "TURNOVER CHARGE", "BALLARPUR"]

df = df[~df['name'].isin(symbols_to_skip)]
df = df[df['code'] != "0000"]
df = df[~((df['buy_qty'] != 0) & (df['sell_qty'] != 0))]
# df = df['code'].dropna().astype(float).drop_duplicates().sort_values().astype(int)
# symbols_df_code = symbols_df['code'].dropna().astype(float).drop_duplicates().sort_values().astype(int)


df['name'] = df['code'].map(lambda x: symbols_df[symbols_df['code'] == x]['name'].iloc[0])
df['isin'] = df['code'].map(lambda x: symbols_df[symbols_df['code'] == x]['isin'].iloc[0])
df['type'] = df.apply(lambda row: 'BUY' if row['buy_qty'] > 0 else ('SELL' if row['sell_qty'] > 0 else 'UNKNOWN'), axis=1)
df['available_qty'] = df.groupby('code')['net_qty'].cumsum()
df['net_qty'] = abs(df['net_qty'])
df['ltcg'] = 0.0
df['stcg'] = 0.0

gains_df = calculate_gains(df.copy())

fy_start_date = datetime(2023, 4, 1)
fy_end_date = datetime(2024, 3, 31)
filtered_df = gains_df[(gains_df['ts'] >= fy_start_date) & (gains_df['ts'] <= fy_end_date)]

print(df.head)
