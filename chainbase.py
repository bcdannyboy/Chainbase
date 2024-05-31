import argparse
import datetime
import psycopg2
import requests
import logging
import pickle
from threading import Timer
from ratelimit import limits, sleep_and_retry
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("chainbase.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TRADIER_API_URL = "https://api.tradier.com/v1"
FMP_API_URL = "https://financialmodelingprep.com/api/v4/etf-holdings"
RATE_LIMIT = 60  # Number of requests per minute
TRADING_START_HOUR = 9  # Trading starts at 9 AM PST
TRADING_END_HOUR = 16  # Trading ends at 4 PM PST
PST = pytz.timezone('America/Los_Angeles')  # PST timezone

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

@sleep_and_retry
@limits(calls=RATE_LIMIT, period=60)
def get_latest_etf_holding_date(symbol, api_key):
    url = f"https://financialmodelingprep.com/api/v4/etf-holdings/portfolio-date"
    params = {'symbol': symbol, 'apikey': api_key}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    if data:
        latest_date = data[0]['date']
        logger.info(f"Latest holding date for {symbol}: {latest_date}")
        return latest_date
    else:
        logger.warning(f"No holding dates found for ETF: {symbol}")
        return None

@sleep_and_retry
@limits(calls=RATE_LIMIT, period=60)
def get_etf_holdings(symbol, date, api_key):
    url = f"{FMP_API_URL}?symbol={symbol}&date={date}"
    response = requests.get(url, params={'apikey': api_key})
    response.raise_for_status()
    data = response.json()
    
    # Log the API response
    logger.info(f"ETF holdings response for {symbol} on {date}: {data}")
    
    if isinstance(data, list):
        holdings = []
        for holding in data:
            if 'symbol' in holding:
                holdings.append(holding['symbol'])
            else:
                logger.warning(f"Missing 'symbol' in holding: {holding}")
        return holdings
    else:
        logger.warning(f"No valid holdings data found for ETF: {symbol}")
        return []

# Function to set up the PostgreSQL database
def setup_database(db_name, user, password, host, port, drop_table):
    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    if drop_table:
        cur.execute("DROP TABLE IF EXISTS options_chains;")
        logger.info("Dropping existing options_chains table.")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS options_chains (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            expiration DATE,
            options BYTEA,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database setup complete.")

# Function to fetch and store options data
def fetch_and_store_options(tickers, db_name, user, password, host, port, tradier_api_key, fmp_api_key):
    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    processed_tickers = set()

    for ticker in tickers:
        if ticker.endswith('.ETF'):
            logger.info(f"Processing ETF: {ticker}")
            etf_symbol = ticker.split('.')[0]
            latest_date = get_latest_etf_holding_date(etf_symbol, fmp_api_key)
            if latest_date:
                etf_holdings = get_etf_holdings(etf_symbol, latest_date, fmp_api_key)
                logger.info(f"Found {len(etf_holdings)} holdings for ETF: {etf_symbol}")
                for holding in etf_holdings:
                    if holding:
                        logger.info(f"Processing ETF holding: {holding}")
                        if holding not in processed_tickers:
                            processed_tickers.add(holding)
                            process_ticker(holding, cur, tradier_api_key)
                    else:
                        logger.warning(f"Skipping invalid holding: {holding}")
        else:
            if ticker not in processed_tickers:
                logger.info(f"Processing ticker: {ticker}")
                processed_tickers.add(ticker)
                process_ticker(ticker, cur, tradier_api_key)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Options data fetched and stored.")

def process_ticker(ticker, cur, api_key):
    try:
        expirations = get_option_expirations(ticker, api_key)
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error fetching expirations for {ticker}: {e}")
        return
    
    for exp in expirations:
        exp_date = PST.localize(datetime.datetime.strptime(exp, '%Y-%m-%d'))
        dte = (exp_date - datetime.datetime.now(PST)).days
        if 1 <= dte <= 90:
            puts, calls = get_option_chain(ticker, exp, api_key)

            options_data = {
                'puts': puts,
                'calls': calls
            }

            logger.info(f"Found {len(puts)} puts and {len(calls)} calls for {ticker} on {exp_date}")
            # Serialize the options data
            pickled_data = pickle.dumps(options_data)

            cur.execute("""
                INSERT INTO options_chains (symbol, expiration, options)
                VALUES (%s, %s, %s)
            """, (ticker, exp_date, pickled_data))

# Function to check if it's within regular trading hours
def is_trading_hours():
    now = datetime.datetime.now(PST)
    return TRADING_START_HOUR <= now.hour < TRADING_END_HOUR

# Function to fetch options data periodically
def schedule_fetch(tickers, db_name, user, password, host, port, tradier_api_key, fmp_api_key, interval):
    fetch_and_store_options(tickers, db_name, user, password, host, port, tradier_api_key, fmp_api_key)  # Always do initial fetch
    if is_trading_hours():
        fetch_and_store_options(tickers, db_name, user, password, host, port, tradier_api_key, fmp_api_key)
    else:
        logger.info("Outside of trading hours. Skipping fetch.")
    Timer(interval, schedule_fetch, args=[tickers, db_name, user, password, host, port, tradier_api_key, fmp_api_key, interval]).start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Set up PostgreSQL server and gather options chains")
    parser.add_argument('--db_name', type=str, required=True, help="Database name")
    parser.add_argument('--user', type=str, required=True, help="Database user")
    parser.add_argument('--password', type=str, required=True, help="Database password")
    parser.add_argument('--host', type=str, default='localhost', help="Database host")
    parser.add_argument('--port', type=int, default=5432, help="Database port")
    parser.add_argument('--tickers', type=str, required=True, help="Comma-separated list of stock tickers and ETFs (e.g., SPY.ETF)")
    parser.add_argument('--interval', type=int, default=3600, help="Interval in seconds to fetch data")
    parser.add_argument('--tradier_api_key', type=str, required=True, help="Tradier API key")
    parser.add_argument('--fmp_api_key', type=str, required=True, help="FMP API key")
    parser.add_argument('--drop_table', action='store_true', help="Drop the table if it exists")

    args = parser.parse_args()
    
    tickers = args.tickers.split(',')
    setup_database(args.db_name, args.user, args.password, args.host, args.port, args.drop_table)
    fetch_and_store_options(tickers, args.db_name, args.user, args.password, args.host, args.port, args.tradier_api_key, args.fmp_api_key)  # Initial fetch
    schedule_fetch(tickers, args.db_name, args.user, args.password, args.host, args.port, args.tradier_api_key, args.fmp_api_key, args.interval)
