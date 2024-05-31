import argparse
import datetime
import psycopg2
from psycopg2.extras import execute_values
import requests
import logging
from threading import Timer
from ratelimit import limits, sleep_and_retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRADIER_API_URL = "https://api.tradier.com/v1"
RATE_LIMIT = 60  # Number of requests per minute

@sleep_and_retry
@limits(calls=RATE_LIMIT, period=60)
def get_option_expirations(symbol, api_token):
    url = f"{TRADIER_API_URL}/markets/options/expirations"
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Accept': 'application/json'
    }
    params = {'symbol': symbol}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return data['expirations']['date']

@sleep_and_retry
@limits(calls=RATE_LIMIT, period=60)
def get_option_chain(symbol, expiration, api_token):
    url = f"{TRADIER_API_URL}/markets/options/chains"
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Accept': 'application/json'
    }
    params = {'symbol': symbol, 'expiration': expiration, 'greeks': 'true'}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    if 'options' in data and 'option' in data['options']:
        options = data['options']['option']
        puts = [opt for opt in options if opt['option_type'] == 'put']
        calls = [opt for opt in options if opt['option_type'] == 'call']
        return puts, calls
    else:
        logger.warning(f"No option data found for {symbol} with expiration {expiration}")
        return [], []

# Function to set up the PostgreSQL database
def setup_database(db_name, user, password, host, port):
    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS options_chains (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            expiration DATE,
            option_type VARCHAR(4),
            strike FLOAT,
            last_trade_date TIMESTAMP,
            bid FLOAT,
            ask FLOAT,
            last_price FLOAT,
            implied_volatility FLOAT,
            volume INT,
            open_interest INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database setup complete.")

# Function to fetch and store options data
def fetch_and_store_options(tickers, db_name, user, password, host, port, api_key):
    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    for ticker in tickers:
        expirations = get_option_expirations(ticker, api_key)
        for exp in expirations:
            exp_date = datetime.datetime.strptime(exp, '%Y-%m-%d')
            dte = (exp_date - datetime.datetime.now()).days
            if 1 <= dte <= 90:
                puts, calls = get_option_chain(ticker, exp, api_key)

                data = []

                for opt in puts:
                    data.append((ticker, exp, 'put', opt['strike'], opt['last_trade_date'], opt['bid'], opt['ask'], opt['last'], opt['implied_volatility'], opt['volume'], opt['open_interest']))

                for opt in calls:
                    data.append((ticker, exp, 'call', opt['strike'], opt['last_trade_date'], opt['bid'], opt['ask'], opt['last'], opt['implied_volatility'], opt['volume'], opt['open_interest']))

                execute_values(cur, """
                    INSERT INTO options_chains (symbol, expiration, option_type, strike, last_trade_date, bid, ask, last_price, implied_volatility, volume, open_interest)
                    VALUES %s
                """, data)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Options data fetched and stored.")

# Function to fetch options data periodically
def schedule_fetch(tickers, db_name, user, password, host, port, api_key, interval):
    fetch_and_store_options(tickers, db_name, user, password, host, port, api_key)
    Timer(interval, schedule_fetch, args=[tickers, db_name, user, password, host, port, api_key, interval]).start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Set up PostgreSQL server and gather options chains")
    parser.add_argument('--db_name', type=str, required=True, help="Database name")
    parser.add_argument('--user', type=str, required=True, help="Database user")
    parser.add_argument('--password', type=str, required=True, help="Database password")
    parser.add_argument('--host', type=str, default='localhost', help="Database host")
    parser.add_argument('--port', type=int, default=5432, help="Database port")
    parser.add_argument('--tickers', type=str, required=True, help="Comma-separated list of stock tickers")
    parser.add_argument('--interval', type=int, default=3600, help="Interval in seconds to fetch data")
    parser.add_argument('--api_key', type=str, required=True, help="Tradier API key")
    
    args = parser.parse_args()
    
    tickers = args.tickers.split(',')
    setup_database(args.db_name, args.user, args.password, args.host, args.port)
    schedule_fetch(tickers, args.db_name, args.user, args.password, args.host, args.port, args.api_key, args.interval)
