import pandas as pd
from datetime import timedelta
import os

def calculate_gains(tx_df, fy_start_date=None, fy_end_date=None, output_file='capital_gains.xlsx'):
    """
    Calculate capital gains with separate tracking for long-term and short-term gains
    and export the results to Excel.
    
    Parameters:
    tx_df (pandas.DataFrame): DataFrame containing transaction data
    fy_start_date (datetime): Start date of the financial year
    fy_end_date (datetime): End date of the financial year
    output_file (str): Path to the output Excel file
    
    Returns:
    tuple: (DataFrame with calculated gains, str: Path to the exported Excel file)
    """
    # Create a copy of the dataframe to avoid modifying the original
    df = tx_df.copy()
    
    # Sort by timestamp to ensure chronological order
    df = df.sort_values(by=['code', 'ts'])
    
    # Initialize columns for tracking
    df['remaining_qty'] = df.apply(lambda row: row['buy_qty'] if row['type'] == 'BUY' else 0, axis=1)
    df['buy_date'] = None
    df['buy_rate'] = None
    df['ltcg'] = 0.0
    df['stcg'] = 0.0
    
    # Create a list to store gain records for Excel output
    gain_records = []
    
    # Group by stock code to process each stock separately
    for code, group in df.groupby('code'):
        buy_queue = []  # Queue to track available BUY transactions
        
        # Process each transaction chronologically
        for idx, row in group.iterrows():
            if row['type'] == 'BUY':
                # Add BUY transaction to the queue
                buy_queue.append({
                    'idx': idx,
                    'date': row['ts'],
                    'qty': row['buy_qty'],
                    'remaining_qty': row['buy_qty'],
                    'rate': row['net_rate'],
                    'isin': row.get('isin', 'UNKNOWN'),
                    'name': row.get('name', 'UNKNOWN')
                })
            elif row['type'] == 'SELL':
                sell_qty = row['sell_qty']
                sell_date = row['ts']
                sell_rate = row['net_rate']
                
                # Match SELL with BUY transactions (FIFO)
                while sell_qty > 0 and buy_queue:
                    buy_tx = buy_queue[0]
                    
                    # Calculate quantity to match
                    match_qty = min(sell_qty, buy_tx['remaining_qty'])
                    
                    # Calculate holding period
                    holding_period = (sell_date - buy_tx['date']).days
                    
                    # Calculate gain for this match
                    gain = match_qty * (sell_rate - buy_tx['rate'])
                    
                    # Update the dataframe with gain information
                    if holding_period > 365:  # Long-term capital gain
                        df.at[idx, 'ltcg'] += gain
                    else:  # Short-term capital gain
                        df.at[idx, 'stcg'] += gain
                    
                    # Record buy information for reference
                    if pd.isna(df.at[idx, 'buy_date']):
                        df.at[idx, 'buy_date'] = buy_tx['date']
                        df.at[idx, 'buy_rate'] = buy_tx['rate']
                    
                    # Create a record for Excel output
                    gain_records.append({
                        'ISIN': buy_tx['isin'],
                        'Description of shares sold': buy_tx['name'],
                        'Number of Shares': match_qty,
                        'Date of Purchase (DD/MM/YYYY)': buy_tx['date'].strftime('%d/%m/%Y'),
                        'Total Purchase Value': match_qty * buy_tx['rate'],
                        'Date of Sale (DD/MM/YYYY)': sell_date.strftime('%d/%m/%Y'),
                        'Sale Price per Share': sell_rate,
                        'Total Sale Value': match_qty * sell_rate
                    })
                    
                    # Update remaining quantities
                    sell_qty -= match_qty
                    buy_tx['remaining_qty'] -= match_qty
                    
                    # Update the original buy transaction's remaining quantity in the dataframe
                    df.at[buy_tx['idx'], 'remaining_qty'] = buy_tx['remaining_qty']
                    
                    # Remove buy transaction from queue if fully utilized
                    if buy_tx['remaining_qty'] == 0:
                        buy_queue.pop(0)
    
    # Filter transactions within the specified financial year if provided
    if fy_start_date and fy_end_date:
        df_fy = df[(df['ts'] >= fy_start_date) & (df['ts'] <= fy_end_date)]
        # Also filter gain records by sale date
        gain_records = [
            record for record in gain_records 
            if fy_start_date <= pd.to_datetime(record['Date of Sale (DD/MM/YYYY)'], format='%d/%m/%Y') <= fy_end_date
        ]
    else:
        df_fy = df
    
    # Create a DataFrame from gain records for Excel output
    gains_excel_df = pd.DataFrame(gain_records)
    
    # Sort by date of sale in ascending order
    if not gains_excel_df.empty:
        gains_excel_df['Date of Sale (DD/MM/YYYY)'] = pd.to_datetime(gains_excel_df['Date of Sale (DD/MM/YYYY)'], format='%d/%m/%Y')
        gains_excel_df = gains_excel_df.sort_values(by='Date of Sale (DD/MM/YYYY)')
        # Convert back to string format for Excel
        gains_excel_df['Date of Sale (DD/MM/YYYY)'] = gains_excel_df['Date of Sale (DD/MM/YYYY)'].dt.strftime('%d/%m/%Y')
    
    # Export to Excel directly
    excel_path = None
    if not gains_excel_df.empty:
        gains_excel_df.to_excel(output_file, index=False)
        excel_path = os.path.abspath(output_file)
        print(f"Gains data exported to {excel_path}")
    else:
        print("No gains data to export.")
    
    # Return both the updated transaction dataframe and the path to the Excel file
    return df_fy, excel_path
