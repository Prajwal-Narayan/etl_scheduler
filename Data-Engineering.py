import yfinance as yf
import pandas as pd # type: ignore
import numpy as np # type: ignore

# Define stock tickers
tickers = ["AAPL", "TSLA"]

# Fetch last 30 days of stock data
data = yf.download(tickers, period="30d", interval="1d")

# Reset index first (to move "Date" into a normal column)
data = data.reset_index()

# Rename columns with lowercase and simple format
data.columns = [col.lower() if isinstance(col, str) else f"{col[1].lower()}_{col[0].lower()}" for col in data.columns]

# Print to check output
print(data.head())

# Display column names
print("\nColumn names:", data.columns)

data.head(5)
