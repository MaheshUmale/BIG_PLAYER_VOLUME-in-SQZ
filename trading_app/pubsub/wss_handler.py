import asyncio
import json
import ssl
import websockets
import upstox_client
from google.protobuf.json_format import MessageToDict
import os

# Local imports
from . import MarketDataFeed_pb2 as pb
from trading_app.pubsub.data_handler import process_live_feed
from trading_app.core.instrument_loader import get_instrument_key

def decode_protobuf_message(buffer):
    """Decodes a protobuf message from the WebSocket."""
    try:
        feed_response = pb.FeedResponse()
        feed_response.ParseFromString(buffer)
        return MessageToDict(feed_response)
    except Exception as e:
        print(f"Error decoding protobuf message: {e}")
        return None

# --- WebSocket Connection Management ---
ACTIVE_SUBSCRIPTIONS = set()
WEBSOCKET_CLIENT = None
RECONNECT_ATTEMPTS = 0
MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_DELAY_SECONDS = 5

async def start_wss_connection(access_token):
    """Initializes and maintains the WebSocket connection."""
    global WEBSOCKET_CLIENT, RECONNECT_ATTEMPTS

    api_version = '2.0'
    configuration = upstox_client.Configuration()
    configuration.access_token = access_token
    api_instance = upstox_client.WebsocketApi(upstox_client.ApiClient(configuration))

    while RECONNECT_ATTEMPTS < MAX_RECONNECT_ATTEMPTS:
        try:
            print("Authorizing WebSocket feed...")
            api_response = api_instance.get_market_data_feed_authorize(api_version)
            wss_url = api_response.data.authorized_redirect_uri

            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with websockets.connect(wss_url, ssl=ssl_context) as websocket:
                WEBSOCKET_CLIENT = websocket
                RECONNECT_ATTEMPTS = 0 # Reset on successful connection
                print("WebSocket connection established. Listening for data...")

                # If there are any pending subscriptions, resubscribe
                if ACTIVE_SUBSCRIPTIONS:
                    print(f"Resubscribing to {len(ACTIVE_SUBSCRIPTIONS)} instruments...")
                    await WEBSOCKET_CLIENT.send(json.dumps({
                        "guid": "resub-guid",
                        "method": "sub",
                        "data": {"mode": "ltpc", "instrumentKeys": list(ACTIVE_SUBSCRIPTIONS)}
                    }).encode('utf-8'))

                while True:
                    message = await websocket.recv()
                    decoded_data = decode_protobuf_message(message)
                    if decoded_data and 'feeds' in decoded_data:
                        process_live_feed(decoded_data['feeds'])

        except upstox_client.rest.ApiException as e:
            print(f"Upstox API Error during WebSocket setup: {e}. Cannot connect.")
            break # Fatal error, stop trying
        except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
            print(f"WebSocket connection closed: {e}.")
            RECONNECT_ATTEMPTS += 1
            print(f"Attempting to reconnect ({RECONNECT_ATTEMPTS}/{MAX_RECONNECT_ATTEMPTS})...")
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        except Exception as e:
            print(f"An unexpected WebSocket error occurred: {e}")
            RECONNECT_ATTEMPTS += 1
            print(f"Attempting to reconnect ({RECONNECT_ATTEMPTS}/{MAX_RECONNECT_ATTEMPTS})...")
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    print("Max reconnect attempts reached. WebSocket service is stopping.")

async def subscribe_to_symbol(symbol):
    """Subscribes to a symbol's live feed if not already subscribed."""
    global ACTIVE_SUBSCRIPTIONS, WEBSOCKET_CLIENT

    instrument_key = get_instrument_key(symbol)
    if not instrument_key:
        print(f"Could not find instrument key for {symbol}. Subscription failed.")
        return

    if instrument_key in ACTIVE_SUBSCRIPTIONS:
        # print(f"Already subscribed to {symbol} ({instrument_key}).")
        return

    if not WEBSOCKET_CLIENT or not WEBSOCKET_CLIENT.open:
        print(f"WebSocket not connected. Adding {symbol} to pending subscriptions.")
        ACTIVE_SUBSCRIPTIONS.add(instrument_key)
        return

    try:
        subscription_message = {
            "guid": f"sub-{symbol}",
            "method": "sub",
            "data": {
                "mode": "ltpc",
                "instrumentKeys": [instrument_key]
            }
        }
        await WEBSOCKET_CLIENT.send(json.dumps(subscription_message).encode('utf-8'))
        ACTIVE_SUBSCRIPTIONS.add(instrument_key)
        print(f"Successfully subscribed to {symbol} ({instrument_key}).")
    except Exception as e:
        print(f"Failed to subscribe to {symbol}: {e}")

def handle_scanner_alert(symbol):
    """
    Endpoint function called by the scanner to trigger a subscription.
    It creates a task to run the subscription in the current event loop.
    """
    # This check prevents creating tasks if the loop is not running,
    # which can happen during shutdown or in certain testing scenarios.
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(subscribe_to_symbol(symbol))
    except RuntimeError:
        print("Event loop is not running. Cannot schedule subscription for {symbol}.")


if __name__ == '__main__':
    # This block is for testing the WebSocket handler independently.
    async def test_main():
        # 1. Get Upstox Access Token (requires environment variable)
        UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")
        if not UPSTOX_ACCESS_TOKEN:
            print("UPSTOX_ACCESS_TOKEN environment variable not set. Cannot run test.")
            return

        # 2. Initialize the database (so candle saving works)
        from trading_app.core.database import init_db
        init_db()

        # 3. Start the WebSocket connection in the background
        print("Starting WebSocket connection task...")
        wss_task = asyncio.create_task(start_wss_connection(UPSTOX_ACCESS_TOKEN))

        # 4. Wait for the connection to be established
        await asyncio.sleep(5)

        # 5. Simulate a scanner alert to trigger a subscription
        test_symbol = "NSE:RELIANCE"
        print(f"\n--- Simulating scanner alert for {test_symbol} ---")
        handle_scanner_alert(test_symbol)

        # 6. Keep the main function running to listen for messages
        await wss_task

    # To run this test:
    # 1. Set the UPSTOX_ACCESS_TOKEN environment variable.
    # 2. Run this file directly: python -m trading_app.pubsub.wss_handler
    print("Running wss_handler.py in test mode...")
    asyncio.run(test_main())