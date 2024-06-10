import requests
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

API_BASE_URL = "https://api.pro.coins.ph/openapi"

def get_markets():
    response = requests.get(f"{API_BASE_URL}/v1/exchangeInfo")

    markets = response.json()['symbols']

    symbols = [market['symbol'] for market in markets if market['quoteAsset'] == 'PHP']
    return symbols

def get_historical_data(symbol, interval, limit):
    response = requests.get(f'{API_BASE_URL}/quote/v1/klines?symbol={symbol}&interval={interval}&limit={limit}')
    
    if not response.ok: 
        return pd.DataFrame([], columns=['close']) 

    df = pd.DataFrame(response.json(), columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 
        'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
    ])

    columns=['open', 'high', 'low', 'close']
    df = df[columns]

    numeric_columns = ['open', 'high', 'low', 'close']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)

    return df

def calculate_sma(data, period):
    if len(data) < period:
        return 0
    return data[-period:].mean()

def screener():
    symbols = get_markets()
    cryptos = []

    for symbol in symbols:
        
        df = get_historical_data(symbol, '1d', 200)

        if df.empty:
            continue

        sma_200 = calculate_sma(df['close'], 200)
        sma_150 = calculate_sma(df['close'], 150)
        sma_50 = calculate_sma(df['close'], 50)

        current_price = df['close'].iloc[-1]

        if sma_200 > sma_150:
            continue

        if sma_150 > sma_50:
            continue
        
        if sma_50 > current_price:
            continue

        cryptos.append(symbol)
    
    df_cryptos  = pd.DataFrame(cryptos, columns=['Cryptocurrencies'])
    return df_cryptos 


print(screener())
