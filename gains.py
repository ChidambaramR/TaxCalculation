from dateutil.relativedelta import relativedelta

def calculate_gains(df):
    for i, sell_row in df[df['type'] == 'SELL'].iterrows():
        stock = sell_row['code']
        sell_qty = sell_row['net_qty']
        sell_rate = sell_row['sell_rate']
        sell_date = sell_row['ts']
        
        # Iterate through buy transactions to match sell quantities
        for j, buy_row in df[(df['type'] == 'BUY') & (df['code'] == stock)].iterrows():
            buy_qty = buy_row['net_qty']
            buy_date = buy_row['ts']
            buy_rate = df.at[j, 'buy_rate']
            
            if sell_qty > 0 and buy_qty > 0:
                if sell_date > buy_date + relativedelta(years=1):
                    gains = 'ltcg'
                else:
                    gains = 'stcg'

                if buy_qty >= sell_qty:
                    # Reduce the buy quantity and set sell quantity to 0
                    df.at[j, 'net_qty'] -= sell_qty
                    df.at[i, gains] = df.at[i, gains] + ((sell_qty * sell_rate) - (sell_qty * buy_rate))
                    sell_qty = 0
                else:
                    # Use up the entire buy quantity
                    df.at[j, 'net_qty'] = 0
                    df.at[i, gains] = df.at[i, gains] + ((buy_qty * sell_rate) - (buy_qty * buy_rate))
                    sell_qty -= buy_qty

            if sell_qty == 0:
                break

    return df