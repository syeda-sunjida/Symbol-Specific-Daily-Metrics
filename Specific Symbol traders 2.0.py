import time
import pandas as pd
from sqlalchemy import create_engine

# Database connection details
db_config = {
    'user': 'readonly_user',
    'password': 'password123',
    'host': 'fn-prod-db-cluster.cluster-ro-cqtlpb5sm2vt.ap-northeast-1.rds.amazonaws.com',
    'database': 'api_backend',
    'port': 3306
}

# Create the connection string
connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

# List of symbols to analyze
symbols = ['EURUSD','BTCUSD','XAUUSD'] 
start_time = "2025-02-14 00:00:00"
end_time = "2025-02-14 23:59:59"

# Start measuring time
script_start_time = time.time()

# Initialize engine outside try block
engine = None
try:
    print("Initializing database connection...")
    engine = create_engine(connection_string)

    # Prepare a multi-tab Excel writer
    with pd.ExcelWriter("Multi_Symbol_Trades_Summary.xlsx", engine="openpyxl") as writer:
        for symbol in symbols:
            print(f"Processing trades for {symbol}...")

            # Step 1: Fetch logins who traded the current symbol at least once in the date range
            symbol_trades_query = f"""
            SELECT DISTINCT login
            FROM trades
            WHERE symbol = '{symbol}'
              AND (
                  open_time BETWEEN UNIX_TIMESTAMP('{start_time}') AND UNIX_TIMESTAMP('{end_time}')
                  OR close_time BETWEEN UNIX_TIMESTAMP('{start_time}') AND UNIX_TIMESTAMP('{end_time}')
              );
            """
            symbol_logins_df = pd.read_sql(symbol_trades_query, engine)
            symbol_logins = symbol_logins_df['login'].tolist()
            print(f"Found {len(symbol_logins)} logins who traded {symbol} at least once.")

            if not symbol_logins:
                print(f"No logins found trading {symbol} in the specified time range.")
                continue

            print(f"Fetching account details for {symbol} logins...")
            symbol_logins_tuple = tuple(symbol_logins)
            account_query = f"""
            SELECT login, type AS type_account, equity, breachedby AS breached, starting_balance, customer_id
            FROM accounts
            WHERE login IN {symbol_logins_tuple};
            """
            accounts_df = pd.read_sql(account_query, engine)

            # Fetch customer details for the accounts
            print("Fetching customer details for the accounts...")
            customer_ids = tuple(int(x) for x in accounts_df['customer_id'].dropna())
            customers_query = f"""
            SELECT id AS customer_id, email
            FROM customers
            WHERE id IN {customer_ids};
            """
            customers_df = pd.read_sql(customers_query, engine)
            print(f"Fetched {len(customers_df)} customers for {symbol}.")

            # Merge accounts and customers data
            accounts_df = pd.merge(accounts_df, customers_df, on='customer_id', how='left')

            # Calculate PnL (Equity - Starting Balance)
            accounts_df['PnL'] = accounts_df['equity'] - accounts_df['starting_balance']

            # Filter Real Accounts
            real_accounts_df = accounts_df[accounts_df['type_account'].str.contains('real', case=False)]

            # Fetch all trades for current symbol logins
            print(f"Fetching all trades for {symbol} logins in the date range...")
            all_trades_query = f"""
            SELECT login, symbol, profit, type
            FROM trades
            WHERE login IN {symbol_logins_tuple}
              AND (
                  open_time BETWEEN UNIX_TIMESTAMP('{start_time}') AND UNIX_TIMESTAMP('{end_time}')
                  OR close_time BETWEEN UNIX_TIMESTAMP('{start_time}') AND UNIX_TIMESTAMP('{end_time}')
              );
            """
            all_trades_df = pd.read_sql(all_trades_query, engine)

            # Merge trade data with real accounts
            real_trades_df = pd.merge(all_trades_df, real_accounts_df[['login']], on='login', how='inner')

            # Step 3: Calculate Trade Metrics
            trade_metrics = (
                real_trades_df.groupby('login')
                .apply(lambda group: pd.Series({
                    'total_trades': len(group),
                    'symbol_trades': len(group[group['symbol'] == symbol]),
                    'symbol_trade_percentage': (len(group[group['symbol'] == symbol]) / len(group)) * 100,
                    'total_profit_sum': group['profit'].sum(),
                    f'{symbol}_profit_sum': group[group['symbol'] == symbol]['profit'].sum(),
                }))
                .reset_index()
            )

            # Merge trade metrics with real accounts
            login_details_real_df = pd.merge(real_accounts_df, trade_metrics, on='login', how='left')

            # Calculate Summary
            total_real_profit = real_trades_df['profit'].sum()
            total_symbol_real_profit = real_trades_df[real_trades_df['symbol'] == symbol]['profit'].sum()
            real_summary_data = {
                'Metric': [
                    'Active Account Count',
                    'Breached Account Count',
                    'Positive PnL Logins Count',
                    'Negative PnL Logins Count',
                    'Total Real Account Profit Sum',
                    f'Total {symbol} Profit Sum (Real Accounts)'
                ],
                'Value': [
                    real_accounts_df['breached'].isna().sum(),  # Active accounts
                    real_accounts_df['breached'].notna().sum(),  # Breached accounts
                    real_accounts_df[real_accounts_df['PnL'] > 0]['login'].nunique(),  # Positive PnL logins
                    real_accounts_df[real_accounts_df['PnL'] <= 0]['login'].nunique(),  # Negative PnL logins
                    total_real_profit,  # Total Real Account Profit Sum
                    total_symbol_real_profit  # Total Symbol Profit Sum (Real Accounts)
                ]
            }

            # Convert summary to DataFrame
            summary_df = pd.DataFrame(real_summary_data)

            # Save to Excel file (separate sheets for each symbol)
            login_details_real_df.to_excel(writer, sheet_name=f"{symbol} Login Details", index=False)
            summary_df.to_excel(writer, sheet_name=f"{symbol} Summary", index=False)

            print(f"Details and summary for {symbol} saved.")

            # Fetch trades for <symbol> logins from the Login Details tab
            print(f"Fetching trades for {symbol} logins in the specified date range...")
            trades_query = f"""
            SELECT 
                login,
                FROM_UNIXTIME(open_time) AS open_time_str,
                ticket,
                type_str,
                lots AS FinalLot,
                symbol,
                open_price,
                sl,
                tp,
                FROM_UNIXTIME(close_time) AS close_time_str,
                close_price,
                commission,
                swap,
                profit
            FROM trades
            WHERE login IN {tuple(real_accounts_df['login'].tolist())}
              AND symbol = '{symbol}'
              AND (
                  open_time BETWEEN UNIX_TIMESTAMP('{start_time}') AND UNIX_TIMESTAMP('{end_time}')
                  OR close_time BETWEEN UNIX_TIMESTAMP('{start_time}') AND UNIX_TIMESTAMP('{end_time}')
              );
            """
            filtered_trades_df = pd.read_sql(trades_query, engine)

            # Save the filtered trades to a new Excel sheet
            filtered_trades_df.to_excel(writer, sheet_name=f"{symbol} Trades", index=False)
            print(f"Filtered trades for {symbol} saved.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if engine is not None:
        engine.dispose()
        print("Database connection closed.")

# End measuring time
script_end_time = time.time()
print(f"Time taken to run the script: {script_end_time - script_start_time} seconds")
