Overview
This Python script extracts trading data from a MySQL database for multiple symbols (e.g., EURUSD, BTCUSD, XAUUSD) over a specific date range. It generates an Excel report summarizing:

- Active and breached trading accounts
- Profit & Loss (PnL) for real accounts
- Trade metrics (e.g., total trades, profit by symbol)
- Detailed trade logs for each symbol
Prerequisites
- Ensure the following dependencies are installed before running the script:


How It Works
1. Database Connection
- Establishes a connection to a MySQL database using SQLAlchemy.
- Uses masked credentials (db_config) for security.
- Fetches data using SQL queries.
2. Symbol Loop Processing
- Iterates through the predefined list of symbols (EURUSD, BTCUSD, XAUUSD).
- Fetches trading logins who have traded each symbol in the given date range (2025-02-14).
3. Fetching Account Details
- Retrieves account details (e.g., type_account, equity, breached status, starting_balance).
- Links accounts to customer details (email).
- Calculates PnL (Equity - Starting Balance) for each trader.
4. Trade Data Extraction
- Retrieves all trades executed by the fetched logins for each symbol.
- Separates real accounts from the fetched data.
- Calculates total profit per symbol and total PnL per login.
5. Summary Metrics Calculation
- Generates key metrics:
- Active Account Count (accounts that are not breached)
- Breached Account Count
- Number of Positive & Negative PnL Logins
- Total Real Account Profit Sum
- Total Profit from the Specific Symbol
6. Generating Excel Output
- Saves data in a multi-sheet Excel file (Multi_Symbol_Trades_Summary.xlsx).
- Each symbol has three sheets:
- Login Details (PnL and trade count per login)
- Summary (high-level metrics)
- Filtered Trades (all trades of the symbol for the logins)

![image](https://github.com/user-attachments/assets/7dc60bc1-4e85-4559-b335-1d715a08cc75)
