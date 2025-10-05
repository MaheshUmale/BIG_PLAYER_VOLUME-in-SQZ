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

async def start_wss_connection(access_token, broadcast_callback):
    """Initializes and maintains the WebSocket connection using v3 API."""
    global WEBSOCKET_CLIENT, RECONNECT_ATTEMPTS

    # The API version for the authorization call must match the token version (v2)
    api_version = '2.0'
    configuration = upstox_client.Configuration()
    configuration.access_token = access_token

    # It's good practice to use a specific API client if available,
    # but WebsocketApi should work for authorization.
    api_instance = upstox_client.WebsocketApi(upstox_client.ApiClient(configuration))

    while RECONNECT_ATTEMPTS < MAX_RECONNECT_ATTEMPTS:
        try:
            print("Authorizing WebSocket feed using v3 endpoint...")
            # Use the v3 authorization method, but with a v2 token
            api_response = api_instance.get_market_data_feed_authorize_v3(api_version)
            wss_url = api_response.data.authorized_redirect_uri

            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with websockets.connect(wss_url, ssl=ssl_context) as websocket:
                WEBSOCKET_CLIENT = websocket
                RECONNECT_ATTEMPTS = 0 # Reset on successful connection
                print("WebSocket v3 connection established. Listening for data...")

                # Resubscribe to any instruments that were active before disconnection
                if ACTIVE_SUBSCRIPTIONS:
                    print(f"Resubscribing to {len(ACTIVE_SUBSCRIPTIONS)} instruments...")
                    await WEBSOCKET_CLIENT.send(json.dumps({
                        "guid": "resub-guid",
                        "method": "sub",
                        "data": {"mode": "ltpc", "instrumentKeys": list(ACTIVE_SUBSCRIPTIONS)}
                    }))

                while True:
                    message = await websocket.recv()
                    decoded_data = decode_protobuf_message(message)
                    if decoded_data and 'feeds' in decoded_data:
                        # Pass the callback to the data handler
                        await process_live_feed(decoded_data['feeds'], broadcast_callback)

        except upstox_client.rest.ApiException as e:
            print(f"Upstox API Error during WebSocket setup: {e}. Cannot connect.")
            break # Fatal API error, stop retrying
        except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
            print(f"WebSocket connection closed: {e}.")
        except Exception as e:
            print(f"An unexpected WebSocket error occurred: {e}")

        # Common logic for reconnection
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
        return

    ACTIVE_SUBSCRIPTIONS.add(instrument_key)

    if not WEBSOCKET_CLIENT or not WEBSOCKET_CLIENT.open:
        print(f"WebSocket not connected. Subscription for {symbol} is pending.")
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
        await WEBSOCKET_CLIENT.send(json.dumps(subscription_message))
        print(f"Successfully subscribed to {symbol} ({instrument_key}).")
    except Exception as e:
        print(f"Failed to subscribe to {symbol}: {e}")

def handle_scanner_alert(symbol):
    """
    Endpoint function called by the scanner to trigger a subscription.
    """
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            loop.create_task(subscribe_to_symbol(symbol))
        else:
            print(f"Event loop not running. Cannot schedule subscription for {symbol}.")
    except RuntimeError:
        print(f"Event loop not running. Cannot schedule subscription for {symbol}.")

# Test block remains the same for standalone testing
if __name__ == '__main__':
    async def test_main():
        UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")
        if not UPSTOX_ACCESS_TOKEN:
            print("UPSTOX_ACCESS_TOKEN environment variable not set. Cannot run test.")
            return

        from trading_app.core.database import init_db
        init_db()

        # Dummy callback for testing purposes
        async def dummy_broadcast(message):
            print(f"--- DUMMY BROADCAST ---: {message}")

        print("Starting WebSocket connection task...")
        # Pass the dummy callback to the connection function
        wss_task = asyncio.create_task(start_wss_connection(UPSTOX_ACCESS_TOKEN, dummy_broadcast))

        await asyncio.sleep(5)

        test_symbol = "NSE_EQ|INE002A01018" # Example: Infosys
        print(f"\n--- Simulating scanner alert for {test_symbol} ---")
        handle_scanner_alert(test_symbol)

        await wss_task

    print("Running wss_handler.py in test mode...")
    asyncio.run(test_main())