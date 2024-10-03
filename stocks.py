import requests
import sqlite3
import json
import time
from datetime import datetime

API_KEY = 'your_alpha_vantage_api_key'
BASE_URL = 'https://www.alphavantage.co/query'

STOCKS = ['SPY','QQQ','NVDA','AAPL','AMZN']  # Using SPY for S&P 500 and DIA for NASDAQ


def fetch_stock_data(symbol):
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    return data


def create_database():
    conn = sqlite3.connect('stock_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_prices (
           
            symbol TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            UNIQUE(symbol, date) -- Ensure uniqueness
        )
    ''')
    conn.commit()
    conn.close()


def insert_data(symbol, data):
    conn = sqlite3.connect('stock_data.db')
    cursor = conn.cursor()

    time_series = data.get('Time Series (Daily)', {})

    for date, prices in time_series.items():
        formatted_date = datetime.strptime(date, '%Y-%m-%d').date()

        cursor.execute('''
            SELECT COUNT(*) FROM stock_prices WHERE symbol = ? AND date = ?
        ''', (symbol, str(formatted_date)))

        if cursor.fetchone()[0] == 0:
            cursor.execute('''
                INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
            symbol, str(formatted_date), float(prices['1. open']), float(prices['2. high']), float(prices['3. low']),
            float(prices['4. close']), int(prices['5. volume'])))
       # else:
        #   print(f"Record for {symbol} on {date} already exists. Skipping.")

    conn.commit()
    conn.close()


def main():
    create_database()

    for stock in STOCKS:
        symbol = stock.split(':')[0] if ':' in stock else stock
        print(f'Fetching data for {stock}...')
        data = fetch_stock_data(symbol)
        insert_data(symbol, data)
        time.sleep(1)  # Alpha Vantage free tier limits: 5 requests per minute

    print('Data loaded into database successfully!')


if __name__ == '__main__':
    main()
