import upstox_client
from upstox_client.rest import ApiException
from datetime import datetime, date
import pandas as pd
from trading_app.pubsub.data_handler import store_candle_data

def get_historical_candle_data(instrument_key, interval, to_date, from_date, access_token):
    """
    Fetches historical candle data from the Upstox API.

    Args:
        instrument_key (str): The Upstox instrument key.
        interval (str): The interval of the candles (e.g., '1minute', 'day').
        to_date (str): The end date in 'YYYY-MM-DD' format.
        from_date (str): The start date in 'YYYY-MM-DD' format.
        access_token (str): The Upstox API access token.

    Returns:
        The API response object from the Upstox client, or None on failure.
    """
    configuration = upstox_client.Configuration()
    configuration.access_token = access_token
    api_client = upstox_client.ApiClient(configuration)
    history_api = upstox_client.HistoryApi(api_client)

    try:
        api_response = history_api.get_historical_candle_data(
            instrument_key=instrument_key,
            interval=interval,
            to_date=to_date,
            from_date=from_date,
            api_version='2.0'
        )
        return api_response
    except ApiException as e:
        print(f"Upstox API exception when fetching historical data: {e}")
        return None

def fill_missing_intraday_data(db, app_id, symbol, instrument_key, start_date, end_date, access_token):
    """
    Fetches historical 1-minute intraday data from Upstox and stores it in Firestore.

    Args:
        db: The Firestore client.
        app_id (str): The application ID.
        symbol (str): The trading symbol (e.g., "NSE:RELIANCE").
        instrument_key (str): The Upstox instrument key.
        start_date (str): The start date in 'YYYY-MM-DD' format.
        end_date (str): The end date in 'YYYY-MM-DD' format.
        access_token (str): The Upstox API access token.
    """
    print(f"Fetching historical data for {symbol} from {start_date} to {end_date}...")
    api_response = get_historical_candle_data(
        instrument_key=instrument_key,
        interval='1minute',
        to_date=end_date,
        from_date=start_date,
        access_token=access_token
    )

    if api_response and api_response.status == 'success' and api_response.data.candles:
        candles_data = api_response.data.candles
        print(f"Fetched {len(candles_data)} historical candles.")

        # The API returns a list of lists. Convert to a list of dicts.
        # Format: [timestamp, open, high, low, close, volume, open_interest]
        for candle in candles_data:
            candle_dict = {
                'timestamp': candle[0],
                'open': candle[1],
                'high': candle[2],
                'low': candle[3],
                'close': candle[4],
                'volume': candle[5],
                'ltq': 0  # Historical data does not provide last traded quantity
            }
            store_candle_data(db, app_id, symbol, candle_dict)

        print("Successfully stored historical data.")
        return True
    else:
        print(f"No historical data found for {symbol} for the given range.")
        return False


if __name__ == '__main__':
    # This block is for demonstration purposes.
    # To run this, you would need to set up your environment variables.

    async def main():
        from trading_app.core.firebase_setup import initialize_firebase
        from trading_app.core.instrument_loader import get_instrument_key
        import os

        try:
            db, auth, app_id, user_id = initialize_firebase()
            UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")

            if not UPSTOX_ACCESS_TOKEN:
                print("UPSTOX_ACCESS_TOKEN is not set.")
                return

            test_symbol = "NSE:RELIANCE"
            instrument_key = get_instrument_key(db, app_id, test_symbol)

            if not instrument_key:
                print(f"Instrument key for {test_symbol} not found in Firestore.")
                # You might need to run the instrument loader seeding first.
                return

            today = date.today().strftime("%Y-%m-%d")

            # This will fetch and fill data for the entire day up to the current time.
            fill_missing_intraday_data(
                db=db,
                app_id=app_id,
                symbol=test_symbol,
                instrument_key=instrument_key,
                start_date=today,
                end_date=today,
                access_token=UPSTOX_ACCESS_TOKEN
            )

        except (ValueError, FileNotFoundError) as e:
            print(f"Firebase initialization failed: {e}")
        except Exception as e:
            print(f"An error occurred in main: {e}")

    # To run this example:
    # 1. Ensure FIREBASE_CONFIG and UPSTOX_ACCESS_TOKEN are set.
    # 2. Ensure your Firestore has the instrument mapping for "NSE:RELIANCE".
    # import asyncio
    # asyncio.run(main())
    pass