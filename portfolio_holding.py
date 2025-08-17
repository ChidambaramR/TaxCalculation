import pandas as pd
from datetime import datetime
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
df = df[df['name'] != "SEBITOC"]
df = df[~((df['buy_qty'] != 0) & (df['sell_qty'] != 0))]
# df = df['code'].dropna().astype(float).drop_duplicates().sort_values().astype(int)
# symbols_df_code = symbols_df['code'].dropna().astype(float).drop_duplicates().sort_values().astype(int)


# Safely map codes to names and ISINs, keeping original name if no match is found
def safe_map_name(x, original_name):
    matches = symbols_df[symbols_df['code'] == x]['name']
    return matches.iloc[0] if not matches.empty else original_name

def safe_map_isin(x):
    matches = symbols_df[symbols_df['code'] == x]['isin']
    return matches.iloc[0] if not matches.empty else 'UNKNOWN'

# Keep original name if no match is found in symbols_df
df['name'] = df.apply(lambda row: safe_map_name(row['code'], row['name']), axis=1)
df['isin'] = df['code'].map(safe_map_isin)
df['type'] = df.apply(lambda row: 'BUY' if row['buy_qty'] > 0 else ('SELL' if row['sell_qty'] > 0 else 'UNKNOWN'), axis=1)
df['available_qty'] = df.groupby('code')['net_qty'].cumsum()
df['net_qty'] = abs(df['net_qty'])
df['ltcg'] = 0.0
df['stcg'] = 0.0

# Define financial year dates
fy_start_date = datetime(2024, 4, 1)
fy_end_date = datetime(2025, 3, 31)

# Calculate gains using the new function with financial year parameters
gains_df, excel_path = calculate_gains(df, fy_start_date, fy_end_date)

# Calculate totals directly from the gains dataframe
total_ltcg = gains_df['ltcg'].sum()
total_stcg = gains_df['stcg'].sum()
total_profit = total_ltcg + total_stcg

# Print the results
print(f"\nFinancial Year: {fy_start_date.strftime('%Y')}-{fy_end_date.strftime('%Y')}")
print(f"Total Profit: ₹{total_profit:,.2f}")
print(f"Total LTCG: ₹{total_ltcg:,.2f}")
print(f"Total STCG: ₹{total_stcg:,.2f}")
