import yfinance as yf
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Database connection details
DB_NAME = "stock_data"
DB_USER = "postgres"
DB_PASS = "0009"
DB_HOST = "localhost"  # Change this if using a remote server
DB_PORT = "5432"

# Define stock tickers
tickers = ["AAPL", "TSLA"]

# Step 1: Extract data from Yahoo Finance
def extract_stock_data(tickers):
    """Fetches historical stock data from Yahoo Finance and renames columns properly."""
    data = yf.download(tickers, period="30d", interval="1d")
    data = data.reset_index()  # Move Date to a normal column

    # Print raw column names before renaming (Debugging Step)
    print("\n🔹 Columns before renaming:", list(data.columns))

    # Fix column renaming to correctly handle tuples and strings
    new_columns = []
    for col in data.columns:
        if isinstance(col, tuple):  
            # MultiIndex column case (e.g., ('Open', 'AAPL'))
            new_columns.append(f"{col[1].lower()}_{col[0].lower()}")
        elif isinstance(col, str):  
            # Explicitly rename '_date' or any variation of Date to 'date'
            new_columns.append("date" if col.strip().lower() in ["date", "_date", "date_"] else col.lower())
        else:
            new_columns.append(col)

    data.columns = new_columns  # Assign new column names

    # Forcefully rename '_date' to 'date' if it still exists
    if "_date" in data.columns:
        data.rename(columns={"_date": "date"}, inplace=True)

    # Print cleaned column names after renaming
    print("\n✅ Columns after extraction:", list(data.columns))

    return data

# Step 2: Transform data
def transform_stock_data(data):
    """Processes and cleans stock data."""
    
    # Print columns to debug before melting
    print("\n🔹 Columns before transformation:", list(data.columns))

    # Ensure "date" column exists before melting
    if "date" not in data.columns:
        print("\n❌ ERROR: 'date' column is missing after extraction! Check column names:", data.columns)
        exit(1)  # Exit program to prevent further errors

    # Melt data to normalize format (date, ticker, open, high, low, close, volume)
    data = data.melt(id_vars=["date"], var_name="ticker_metric", value_name="value")

    # Extract ticker and metric (AAPL_Open → AAPL, Open)
    data["ticker"] = data["ticker_metric"].apply(lambda x: x.split("_")[0])
    data["metric"] = data["ticker_metric"].apply(lambda x: x.split("_")[1])

    # Pivot table to get final structure
    data = data.pivot_table(index=["date", "ticker"], columns="metric", values="value").reset_index()

    # Rename columns
    data.columns = ["date", "ticker", "open_price", "high_price", "low_price", "close_price", "volume"]

    # Handle missing values (if any)
    data = data.fillna(0)

    print("\n✅ Transformed Data Sample:\n", data.head())  # Debugging step

    return data

# Step 3: Load data into PostgreSQL
def load_to_postgresql(data):
    """Loads stock data into PostgreSQL."""
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        data.to_sql("stock_prices", engine, if_exists="append", index=False)
        print("\n✅ Data successfully loaded into PostgreSQL")
    except Exception as e:
        print(f"\n❌ Error loading data into PostgreSQL: {e}")

# Run the ETL pipeline
if __name__ == "__main__":
    raw_data = extract_stock_data(tickers)
    transformed_data = transform_stock_data(raw_data)
    load_to_postgresql(transformed_data)
