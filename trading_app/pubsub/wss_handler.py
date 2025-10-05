import asyncio
import json
import ssl
import websockets
import upstox_client
from google.protobuf.json_format import MessageToDict
from datetime import datetime, time
import pandas as pd

# Assuming the protobuf file from the example is available
# In a real project, you would generate this from the .proto file provided by Upstox
from . import MarketDataFeed_pb2 as pb
from trading_app.pubsub.data_handler import store_candle_data
from trading_app.core.instrument_loader import get_instrument_key

# --- Candle Aggregation Logic ---
# This dictionary will hold the state of the current 1-minute candle for each subscribed instrument.
CANDLE_STATE = {}

def decode_protobuf_message(buffer):
    """Decodes a protobuf message from the WebSocket."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return MessageToDict(feed_response)

def process_live_feed(feed, db, app_id):
    """
    Processes a single tick from the live feed and updates the candle state.
    If a 1-minute interval has passed, it stores the completed candle.
    """
    global CANDLE_STATE

    instrument_key = list(feed.keys())[0]
    tick = feed[instrument_key]['ltpc']

    price = tick.get('ltp')
    quantity = tick.get('ltq')
    timestamp_ms = int(tick.get('ltt'))

    if not all([price, quantity, timestamp_ms]):
        return # Skip if essential data is missing

    dt_object = datetime.fromtimestamp(timestamp_ms / 1000)
    current_minute = dt_object.replace(second=0, microsecond=0)

    if instrument_key not in CANDLE_STATE:
        # First tick for this instrument, initialize the candle
        CANDLE_STATE[instrument_key] = {
            'timestamp': current_minute.isoformat(),
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': quantity,
            'ltq': quantity # Store the last traded quantity
        }
        return

    # Update the existing candle state
    state = CANDLE_STATE[instrument_key]
    state_minute = datetime.fromisoformat(state['timestamp'])

    if current_minute > state_minute:
        # A new minute has started, the previous candle is complete
        print(f"Completed 1-min candle for {instrument_key}: {state}")

        # Store the completed candle
        symbol = instrument_key # In a real scenario, you'd map this back to a symbol like "NSE:RELIANCE"
        store_candle_data(db, app_id, symbol, state)

        # Start a new candle for the current minute
        CANDLE_STATE[instrument_key] = {
            'timestamp': current_minute.isoformat(),
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': quantity,
            'ltq': quantity
        }
    else:
        # Continue updating the current candle
        state['high'] = max(state['high'], price)
        state['low'] = min(state['low'], price)
        state['close'] = price
        state['volume'] += quantity
        state['ltq'] = quantity


# --- WebSocket Connection Management ---
ACTIVE_SUBSCRIPTIONS = set()
WEBSOCKET_CLIENT = None

async def start_wss_connection(access_token, db, app_id):
    """Initializes and maintains the WebSocket connection."""
    global WEBSOCKET_CLIENT

    api_version = '2.0'
    configuration = upstox_client.Configuration()
    configuration.access_token = access_token

    api_instance = upstox_client.WebsocketApi(upstox_client.ApiClient(configuration))

    try:
        api_response = api_instance.get_market_data_feed_authorize(api_version)
        wss_url = api_response.data.authorized_redirect_uri

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        async with websockets.connect(wss_url, ssl=ssl_context) as websocket:
            WEBSOCKET_CLIENT = websocket
            print("WebSocket connection established.")

            # Keep the connection alive
            while True:
                message = await websocket.recv()
                decoded_data = decode_protobuf_message(message)
                if 'feeds' in decoded_data:
                    process_live_feed(decoded_data['feeds'], db, app_id)

    except upstox_client.ApiException as e:
        print(f"Upstox API Error: {e}")
    except websockets.exceptions.ConnectionClosed as e:
        print(f"WebSocket connection closed: {e}. Attempting to reconnect...")
        await asyncio.sleep(5)
        await start_wss_connection(access_token, db, app_id) # Reconnect
    except Exception as e:
        print(f"An unexpected WebSocket error occurred: {e}")

async def subscribe_to_symbol(symbol, db, app_id):
    """Subscribes to a symbol's live feed if not already subscribed."""
    global ACTIVE_SUBSCRIPTIONS, WEBSOCKET_CLIENT

    instrument_key = get_instrument_key(db, app_id, symbol)
    if not instrument_key:
        print(f"Could not find instrument key for {symbol}. Subscription failed.")
        return

    if instrument_key in ACTIVE_SUBSCRIPTIONS:
        print(f"Already subscribed to {symbol} ({instrument_key}).")
        return

    if not WEBSOCKET_CLIENT:
        print("WebSocket client not connected. Cannot subscribe.")
        return

    subscription_message = {
        "guid": "some_guid",
        "method": "sub",
        "data": {
            "mode": "ltpc",
            "instrumentKeys": [instrument_key]
        }
    }
    await WEBSOCKET_CLIENT.send(json.dumps(subscription_message).encode('utf-8'))
    ACTIVE_SUBSCRIPTIONS.add(instrument_key)
    print(f"Subscribed to {symbol} ({instrument_key}).")

def handle_scanner_alert(symbol, db, app_id):
    """Endpoint function to be called by the scanner."""
    print(f"Received scanner alert for: {symbol}")
    # Run the subscription in the existing event loop
    asyncio.create_task(subscribe_to_symbol(symbol, db, app_id))


if __name__ == '__main__':
    # This block is for demonstration purposes.
    # In the final application, this will be integrated into a main server process.
    # You would need to provide a valid Upstox access token and Firebase credentials.

    async def main():
        # --- MOCK SETUP ---
        # 1. Initialize Firebase (requires environment variables)
        from trading_app.core.firebase_setup import initialize_firebase
        try:
            db, auth, app_id, user_id = initialize_firebase()
        except (ValueError, FileNotFoundError) as e:
            print(f"Firebase initialization failed: {e}")
            return

        # 2. Get Upstox Access Token (requires environment variable)
        UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")
        if not UPSTOX_ACCESS_TOKEN:
            print("UPSTOX_ACCESS_TOKEN environment variable not set.")
            return

        # 3. Start the WebSocket connection in the background
        wss_task = asyncio.create_task(start_wss_connection(UPSTOX_ACCESS_TOKEN, db, app_id))

        # 4. Wait for the connection to be established
        await asyncio.sleep(5)

        # 5. Simulate a scanner alert to trigger a subscription
        test_symbol = "NSE:RELIANCE"
        handle_scanner_alert(test_symbol, db, app_id)

        # 6. Keep the main function running to listen for messages
        await wss_task

    # To run this:
    # 1. Set FIREBASE_CONFIG env var.
    # 2. Set UPSTOX_ACCESS_TOKEN env var.
    # 3. Make sure you have a valid instrument mapping in Firestore for "NSE:RELIANCE".
    # asyncio.run(main())
    pass