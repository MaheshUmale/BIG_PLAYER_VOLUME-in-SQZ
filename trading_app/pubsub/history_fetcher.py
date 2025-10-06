import upstox_client
from upstox_client.rest import ApiException
from datetime import datetime, date
import pandas as pd
from trading_app.core.database import save_candle_data

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

def fill_missing_intraday_data(symbol, instrument_key, start_date, end_date, access_token):
    """
    Fetches historical 1-minute intraday data from Upstox and stores it in the database.

    Args:
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
        print(f"Fetched {len(candles_data)} historical candles for {symbol}.")

        # The API returns a list of lists.
        # Format: [timestamp, open, high, low, close, volume, open_interest]
        for candle in candles_data:
            candle_dict = {
                'timestamp': candle[0],
                'open': candle[1],
                'high': candle[2],
                'low': candle[3],
                'close': candle[4],
                'volume': candle[5]
            }
            # Save each candle to the database
            save_candle_data(instrument_key, candle_dict)

        print(f"Successfully stored historical data for {symbol}.")
        return True
    else:
        print(f"No historical data found for {symbol} for the given range.")
        return False


# The __main__ block has been removed as it contained Firebase-specific logic.
# The functions in this module are now designed to be called from other parts of the application
# that manage the database connection and Upstox access token.
pass