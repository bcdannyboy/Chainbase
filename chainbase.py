import argparse
import datetime
import time
import psycopg2
from psycopg2.extras import execute_values
import yfinance as yf
import logging
from threading import Timer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
def fetch_and_store_options(tickers, db_name, user, password, host, port):
    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    for ticker in tickers:
        stock = yf.Ticker(ticker)
        options = stock.options
        for exp in options:
            exp_date = datetime.datetime.strptime(exp, '%Y-%m-%d')
            dte = (exp_date - datetime.datetime.now()).days
            if 1 <= dte <= 90:
                options_chain = stock.option_chain(exp)
                puts = options_chain.puts
                calls = options_chain.calls

                data = []

                for _, row in puts.iterrows():
                    data.append((ticker, exp, 'put', row['strike'], row['lastTradeDate'], row['bid'], row['ask'], row['lastPrice'], row['impliedVolatility'], row['volume'], row['openInterest']))

                for _, row in calls.iterrows():
                    data.append((ticker, exp, 'call', row['strike'], row['lastTradeDate'], row['bid'], row['ask'], row['lastPrice'], row['impliedVolatility'], row['volume'], row['openInterest']))

                execute_values(cur, """
                    INSERT INTO options_chains (symbol, expiration, option_type, strike, last_trade_date, bid, ask, last_price, implied_volatility, volume, open_interest)
                    VALUES %s
                """, data)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Options data fetched and stored.")

# Function to fetch options data periodically
def schedule_fetch(tickers, db_name, user, password, host, port, interval):
    fetch_and_store_options(tickers, db_name, user, password, host, port)
    Timer(interval, schedule_fetch, args=[tickers, db_name, user, password, host, port, interval]).start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Set up PostgreSQL server and gather options chains")
    parser.add_argument('--db_name', type=str, required=True, help="Database name")
    parser.add_argument('--user', type=str, required=True, help="Database user")
    parser.add_argument('--password', type=str, required=True, help="Database password")
    parser.add_argument('--host', type=str, default='localhost', help="Database host")
    parser.add_argument('--port', type=int, default=5432, help="Database port")
    parser.add_argument('--tickers', type=str, required=True, help="Comma-separated list of stock tickers")
    parser.add_argument('--interval', type=int, default=3600, help="Interval in seconds to fetch data")
    
    args = parser.parse_args()
    
    tickers = args.tickers.split(',')
    setup_database(args.db_name, args.user, args.password, args.host, args.port)
    schedule_fetch(tickers, args.db_name, args.user, args.password, args.host, args.port, args.interval)
