# Chainbase

Chainbase uses Tradier to create a database of option chains for future analysis and backtesting. This script sets up a PostgreSQL server and periodically gathers options chains for a provided list of stock tickers and ETFs.

## Features

- Sets up a PostgreSQL database to store options chains data.
- Fetches options chains data from 1 DTE to 90 DTE for specified stock tickers using the Tradier API.
- Fetches tickers within ETFs and gathers options chains for each ticker using the Financial Modeling Prep API.
- Ensures tickers are not processed more than once if they overlap between ETFs.
- Stores the fetched options data in a serialized (pickled) format in the PostgreSQL database.
- Schedules periodic fetching of options data (default: once an hour).
- Provides verbose logging to a file for easy monitoring and debugging, even when running in the background with `nohup`.

## Prerequisites

- Python 3.x
- PostgreSQL
- Required Python packages: `psycopg2`, `requests`, `argparse`, `ratelimit`

## Installation

1. **Clone the repository:**

    ```bash
    git clone https://github.com/yourusername/chainbase.git
    cd chainbase
    ```

2. **Install the required Python packages:**

    ```bash
    pip install psycopg2 requests argparse ratelimit
    ```

3. **Set up PostgreSQL:**

    Make sure you have PostgreSQL installed and running. Create a database for storing the options data.

    ```sql
    CREATE DATABASE chainbase;
    ```

## Usage

1. **Set up the database and start fetching options chains:**

    Run the script with the required arguments:

    ```bash
    python chainbase.py --db_name chainbase --user your_db_user --password your_db_password --host localhost --port 5432 --tickers AAPL,MSFT,GOOG,SPY.ETF --interval 3600 --tradier_api_key your_tradier_api_key --fmp_api_key your_fmp_api_key --drop_table
    ```

    - `--db_name`: Name of the PostgreSQL database.
    - `--user`: Database user.
    - `--password`: Database user's password.
    - `--host`: Database host (default: `localhost`).
    - `--port`: Database port (default: `5432`).
    - `--tickers`: Comma-separated list of stock tickers and ETFs (e.g., `SPY.ETF`).
    - `--interval`: Interval in seconds to fetch data (default: `3600` seconds, or 1 hour).
    - `--tradier_api_key`: Tradier API key.
    - `--fmp_api_key`: Financial Modeling Prep API key.
    - `--drop_table`: Drop the existing options table and recreate it.

2. **Run the script in the background with verbose logging:**

    Use `nohup` to run the script in the background and ensure logging to a file:

    ```bash
    nohup python3 chainbase.py --db_name chainbase --user your_db_user --password your_db_password --host localhost --port 5432 --tickers AAPL,MSFT,GOOG,SPY.ETF --interval 3600 --tradier_api_key your_tradier_api_key --fmp_api_key your_fmp_api_key --drop_table > chainbase_nohup.log 2>&1 &
    ```

## Example

To set up the database and start fetching options chains for Apple, Microsoft, Google, and the tickers within the SPY ETF every hour, and run it in the background with verbose logging:

```bash
nohup python3 chainbase.py --db_name chainbase --user your_db_user --password your_db_password --host localhost --port 5432 --tickers AAPL,MSFT,GOOG,SPY.ETF --interval 3600 --tradier_api_key your_tradier_api_key --fmp_api_key your_fmp_api_key --drop_table > chainbase_nohup.log 2>&1 &
```

This setup ensures that the script continues to run and logs its activity to `chainbase_nohup.log` even when you log out or close the terminal.
