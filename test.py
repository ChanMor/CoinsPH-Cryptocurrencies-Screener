import requests
import pandas as pd
import time
import hmac
import hashlib
import urllib.parse
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
API_BASE_URL = os.getenv('API_BASE_URL')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def get_markets():
    response = requests.get(f"{API_BASE_URL}/v1/exchangeInfo")
    if response.ok:
        markets = response.json()['symbols']
        symbols = [market['symbol'] for market in markets if market['quoteAsset'] == 'PHP']
        return symbols
    else:
        print(f"Failed to fetch market info: {response.text}")
        return []

def generate_signature(params):
    query_string = urllib.parse.urlencode(params)
    digest = hmac.new(API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    return digest

def trades(symbol='', start_time='', end_time='', timestamp=''):
    url = f"{API_BASE_URL}/v1/myTrades"
    if not timestamp:
        timestamp = str(int(time.time() * 1000))

    params = {
        'symbol': symbol,
        'startTime': start_time,
        'endTime': end_time,
        'recvWindow': '10000',
        'timestamp': timestamp
    }
    params = {k: v for k, v in params.items() if v}
     
    params['signature'] = generate_signature(params)

    headers = {
        'X-COINS-APIKEY': API_KEY,
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.get(url, headers=headers, params=params)
    
    if response.ok:
        data = response.json()
        if isinstance(data, list) and data:  # Ensure data is a non-empty list
            return pd.DataFrame(data)
        else:
            print(f"Unexpected response format or empty data: {data}")
            return pd.DataFrame()
    else:
        print(f"Failed to fetch order history: {response.text}")
        return pd.DataFrame()

def get_all_trades():
    symbols = get_markets()
    all_trades = pd.DataFrame()

    for symbol in symbols:
        trades_df = trades(symbol=symbol)
        all_trades = pd.concat([all_trades, trades_df], ignore_index=True)
    
    return all_trades

def process_trades(trades_df):
    if trades_df.empty:
        print("No trades data available.")
        return pd.DataFrame()
    
    trades_df['Date'] = pd.to_datetime(trades_df['time'], unit='ms')
    trades_df['Type'] = trades_df['isBuyer'].apply(lambda x: 'Buy' if x else 'Sell')
    trades_df['price'] = trades_df['price'].astype(float)
    trades_df['qty'] = trades_df['qty'].astype(float)
    trades_df['Total Price'] = trades_df['price'] * trades_df['qty']
    trades_df['commission'] = trades_df['commission'].astype(float)
    
    trades_df['Commission (PHP)'] = trades_df.apply(
        lambda row: row['commission'] * row['price'] if row['commissionAsset'] != 'PHP' else row['commission'],
        axis=1
    )
    
    def process_symbol_trades(symbol_trades):
        symbol_trades = symbol_trades.sort_values(by='Date')
        buy_positions = []
        sell_positions = []
        current_buy_position = None
        current_sell_position = None
        sold_quantity = 0

        for _, row in symbol_trades.iterrows():
            if row['Type'] == 'Buy':
                if current_buy_position is None:
                    current_buy_position = {
                        'Date': row['Date'],
                        'Symbol': row['symbol'],
                        'Type': 'Buy',
                        'Average Price': row['price'],
                        'Quantity': row['qty'],
                        'Total Price': row['Total Price'],
                        'Commission (PHP)': row['Commission (PHP)']
                    }
                else:
                    current_buy_position['Average Price'] = (
                        (current_buy_position['Average Price'] * current_buy_position['Quantity'] + row['price'] * row['qty']) /
                        (current_buy_position['Quantity'] + row['qty'])
                    )
                    current_buy_position['Quantity'] += row['qty']
                    current_buy_position['Total Price'] += row['Total Price']
                    current_buy_position['Commission (PHP)'] += row['Commission (PHP)']
            elif row['Type'] == 'Sell':
                sell_qty = row['qty']
                sold_quantity += sell_qty
                sell_total_price = row['price'] * sell_qty
                sell_commission = row['Commission (PHP)']
                
                if current_sell_position is None:
                    current_sell_position = {
                        'Date': row['Date'],
                        'Symbol': row['symbol'],
                        'Type': 'Sell',
                        'Average Price': row['price'],
                        'Quantity': row['qty'],
                        'Total Price': sell_total_price,
                        'Commission (PHP)': sell_commission
                    }
                else:
                    current_sell_position['Average Price'] = (
                        (current_sell_position['Average Price'] * current_sell_position['Quantity'] + row['price'] * row['qty']) /
                        (current_sell_position['Quantity'] + row['qty'])
                    )
                    current_sell_position['Quantity'] += row['qty']
                    current_sell_position['Total Price'] += sell_total_price
                    current_sell_position['Commission (PHP)'] += sell_commission

                if current_buy_position is not None and sold_quantity >= 0.95 * current_buy_position['Quantity']:
                    buy_positions.append(current_buy_position)
                    sell_positions.append(current_sell_position)
                    current_buy_position = None
                    current_sell_position = None
                    sold_quantity = 0

        if current_buy_position is not None:
            buy_positions.append(current_buy_position)
        if current_sell_position is not None:
            sell_positions.append(current_sell_position)

        return pd.concat([pd.DataFrame(buy_positions), pd.DataFrame(sell_positions)], ignore_index=True)

    all_positions = pd.concat([process_symbol_trades(trades_df[trades_df['symbol'] == symbol]) for symbol in trades_df['symbol'].unique()], ignore_index=True)

    return all_positions

def generate_trade_statistics(processed_trades_df):
    stats = []

    for symbol in processed_trades_df['Symbol'].unique():
        symbol_trades = processed_trades_df[processed_trades_df['Symbol'] == symbol].sort_values(by='Date')
        current_buy = None

        for _, row in symbol_trades.iterrows():
            if row['Type'] == 'Buy':
                if current_buy is None:
                    current_buy = row
                else:
                    current_buy['Average Price'] = (
                        (current_buy['Average Price'] * current_buy['Quantity'] + row['Average Price'] * row['Quantity']) /
                        (current_buy['Quantity'] + row['Quantity'])
                    )
                    current_buy['Quantity'] += row['Quantity']
                    current_buy['Total Price'] += row['Total Price']
                    current_buy['Commission (PHP)'] += row['Commission (PHP)']
            elif row['Type'] == 'Sell' and current_buy is not None:
                stats.append({
                    'Date Bought': current_buy['Date'],
                    'Date Sold': row['Date'],
                    'Symbol': row['Symbol'],
                    'Quantity': row['Quantity'],
                    'Average Price Bought (PHP)': current_buy['Average Price'],
                    'Average Price Sold (PHP)': row['Average Price'],
                    'Total Price Bought (PHP)': current_buy['Total Price'],
                    'Total Price Sold (PHP)': row['Total Price'],
                    'Average Gain/Loss (PHP)': row['Total Price'] - current_buy['Total Price'],
                    'Average Gain/Loss (%)': ((row['Total Price'] - current_buy['Total Price']) / current_buy['Total Price']) * 100
                })
                current_buy = None  # Reset the current buy after calculating the gain/loss

    return pd.DataFrame(stats)

# Fetch all trades
all_trades_df = get_all_trades()

# Process trades
processed_trades_df = process_trades(all_trades_df)
trade_statistics_df = generate_trade_statistics(processed_trades_df)

# Display the result
processed_trades_df.to_csv('processed_trades.csv', index=False)
trade_statistics_df.to_csv('trade_statistics.csv', index=False)
print(processed_trades_df)
