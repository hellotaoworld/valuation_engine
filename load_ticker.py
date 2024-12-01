import yfinance as yf

# Example ticker
ticker = "AAPL"  # Replace with the company's ticker

# Fetch stock data
stock = yf.Ticker(ticker)

# Extract exchange information
exchange = stock.info.get("exchange", "Exchange information not available")
print(f"Ticker: {ticker}, Exchange: {exchange}")