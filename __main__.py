import argparse
import os
import sys
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Alpaca API imports
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame


def get_tickers_from_db(engine) -> list:
    """Fetches a list of unique tickers from the existing finviz_data table."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT DISTINCT ticker FROM daily_stock_snapshots;")
            )
            tickers = [row[0] for row in result.fetchall()]
            return tickers
    except Exception as e:
        print(f"Error fetching tickers from database: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    # Load environment variables from the .env file
    load_dotenv()

    api_key = os.getenv("ALPACA_API_KEY")
    api_secret = os.getenv("ALPACA_API_SECRET")

    if not api_key or not api_secret:
        print(
            "Error: ALPACA_API_KEY or ALPACA_API_SECRET not found in .env file.",
            file=sys.stderr,
        )
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Fetch daily OHLCV data from Alpaca for database tickers."
    )
    parser.add_argument(
        "--db-url", required=True, type=str, help="PostgreSQL Connection URL"
    )
    parser.add_argument(
        "--target-date",
        type=str,
        help="Specific date to fetch prices for (YYYY-MM-DD). Defaults to today.",
    )

    args = parser.parse_args()

    # 1. Connect to Database
    print("Connecting to database...")
    engine = create_engine(args.db_url)

    # 2. Get Tickers
    tickers = get_tickers_from_db(engine)
    if not tickers:
        print("No tickers found in the database. Exiting.")
        sys.exit(0)
    print(f"Found {len(tickers)} unique tickers in the database.")

    # 3. Setup Alpaca Client using env variables
    print("Connecting to Alpaca...")
    client = StockHistoricalDataClient(api_key, api_secret)

    # Determine date range
    if args.target_date:
        try:
            # Parse the target date and set time to the very end of the day to ensure we capture its data
            end_dt = datetime.strptime(args.target_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            )
        except ValueError:
            print("Error: --target-date must be in YYYY-MM-DD format.", file=sys.stderr)
            sys.exit(1)
    else:
        end_dt = datetime.today()

    start_dt = end_dt - timedelta(days=4)  # Add 4-day buffer for weekends/holidays

    # 4. Fetch Data in Chunks
    chunk_size = 1000
    all_bars_df = pd.DataFrame()

    for i in range(0, len(tickers), chunk_size):
        # Extract the chunk and replace hyphens with dots for the Alpaca API
        chunk_raw = tickers[i : i + chunk_size]
        chunk_alpaca = [t.replace("-", ".") for t in chunk_raw]

        print(
            f" -> Fetching Alpaca data for batch {i + 1} to {min(i + chunk_size, len(tickers))}..."
        )

        request_params = StockBarsRequest(
            symbol_or_symbols=chunk_alpaca,
            timeframe=TimeFrame.Day,
            start=start_dt,
            end=end_dt,
        )

        try:
            bars = client.get_stock_bars(request_params)
            if bars.data:
                # Convert Alpaca's response directly to a Pandas DataFrame
                chunk_df = bars.df.reset_index()
                all_bars_df = pd.concat([all_bars_df, chunk_df], ignore_index=True)
        except Exception as e:
            print(f"Error fetching data for chunk: {e}", file=sys.stderr)

    if all_bars_df.empty:
        print("No pricing data retrieved from Alpaca.")
        sys.exit(0)

    # 5. Clean and Format Data for PostgreSQL
    all_bars_df.rename(columns={"symbol": "ticker", "timestamp": "date"}, inplace=True)

    # Revert dots back to hyphens so the tickers match your finviz_data table perfectly
    all_bars_df["ticker"] = all_bars_df["ticker"].str.replace(".", "-", regex=False)

    # Extract date and filter to just the latest valid trading date within the chunk
    all_bars_df["date"] = pd.to_datetime(all_bars_df["date"]).dt.date

    latest_date = all_bars_df["date"].max()
    all_bars_df = all_bars_df[all_bars_df["date"] == latest_date]

    # 6. Save to Database
    db_table = "daily_prices"
    print(
        f"Saving {len(all_bars_df)} price records for {latest_date} to table '{db_table}'..."
    )

    try:
        all_bars_df.to_sql(db_table, engine, if_exists="append", index=False)
        print("Successfully saved daily prices to PostgreSQL!")
    except Exception as e:
        print(f"Database export failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
