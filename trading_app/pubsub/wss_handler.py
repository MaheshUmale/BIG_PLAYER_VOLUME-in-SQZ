import asyncio
import json
import ssl
import websockets
import upstox_client
from google.protobuf.json_format import MessageToDict
import os
import http.client
import queue  # Added for subscription queue

# Local imports
from . import MarketDataFeedV3_pb2 as pb
from trading_app.pubsub.data_handler import process_live_feed
from trading_app.core.instrument_loader import get_instrument_key

def decode_protobuf_message(buffer):
    """Decodes a protobuf message from the WebSocket using the V3 schema."""
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

def get_market_data_feed_authorize_url(access_token):
    """Fetches the authorized WebSocket URI from the Upstox V3 REST endpoint."""
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Api-Version": "3.0"
    }
    try:
        conn = http.client.HTTPSConnection("api.upstox.com")
        conn.request("GET", "/v3/feed/market-data-feed/authorize", headers=headers)
        response = conn.getresponse()
        response_data = response.read().decode('utf-8')
        conn.close()
        if response.status == 200:
            auth_response = json.loads(response_data)
            wss_url = auth_response.get("data", {}).get("authorizedRedirectUri")
            if not wss_url:
                print("Error: Could not get authorizedRedirectUri from V3 authorize endpoint.")
                return None
            print(f"Obtained WSS URL from V3 authorize endpoint: {wss_url}")
            return wss_url
        else:
            print(f"Error authorizing V3 WebSocket feed: {response.status} - {response_data}")
            return None
    except Exception as e:
        print(f"Error making HTTP request to V3 authorize endpoint: {e}")
        return None

async def check_subscription_queue(sub_queue):
    """Periodically checks the subscription queue and subscribes to new symbols."""
    print("Subscription queue checker started.")
    while True:
        try:
            symbol = sub_queue.get_nowait()
            if symbol:
                print(f"Found symbol '{symbol}' in queue. Processing subscription.")
                await subscribe_to_symbol(symbol)
        except queue.Empty:
            await asyncio.sleep(1)
        except Exception as e:
            print(f"Error in subscription queue checker: {e}")
            await asyncio.sleep(1)

async def start_wss_connection(access_token, broadcast_callback, subscription_queue):
    """Initializes and maintains the WebSocket connection using v3 API."""
    global WEBSOCKET_CLIENT, RECONNECT_ATTEMPTS

    auth_header = {"Authorization": f"Bearer {access_token}"}
    queue_task = None
    while RECONNECT_ATTEMPTS < MAX_RECONNECT_ATTEMPTS:
        try:
            print("Authorizing WebSocket feed using V3 REST endpoint...")
            wss_url = get_market_data_feed_authorize_url(access_token)
            if not wss_url:
                raise Exception("Failed to get WSS URL, cannot proceed.")

            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with websockets.connect(wss_url, ssl=ssl_context, additional_headers=auth_header) as websocket:
                WEBSOCKET_CLIENT = websocket
                RECONNECT_ATTEMPTS = 0
                print("WebSocket V3 connection established. Listening for data...")

                queue_task = asyncio.create_task(check_subscription_queue(subscription_queue))

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
                        await process_live_feed(decoded_data['feeds'], broadcast_callback)

        except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
            print(f"WebSocket connection closed: {e}.")
        except Exception as e:
            print(f"An unexpected WebSocket error occurred: {e}")
        finally:
            if queue_task:
                queue_task.cancel()
            WEBSOCKET_CLIENT = None
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
    """Endpoint function called by the scanner to trigger a subscription."""
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            loop.create_task(subscribe_to_symbol(symbol))
        else:
            print(f"Event loop not running. Cannot schedule subscription for {symbol}.")
    except RuntimeError:
        print(f"Event loop not running. Cannot schedule subscription for {symbol}.")

# Standalone test block
if __name__ == '__main__':
    async def test_main():
        UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")
        if not UPSTOX_ACCESS_TOKEN:
            print("UPSTOX_ACCESS_TOKEN environment variable not set. Cannot run test.")
            return

        from trading_app.core.database import init_db
        init_db()

        async def dummy_broadcast(message):
            print(f"--- DUMMY BROADCAST ---: {message}")

        test_queue = queue.Queue()
        print("Starting WebSocket connection task...")
        wss_task = asyncio.create_task(start_wss_connection(UPSTOX_ACCESS_TOKEN, dummy_broadcast, test_queue))

        await asyncio.sleep(10)

        test_symbol = "NSE_EQ|INE002A01018"
        print(f"\n--- Simulating client subscription for {test_symbol} via queue ---")
        test_queue.put(test_symbol)

        await wss_task

    print("Running wss_handler.py in test mode...")
    asyncio.run(test_main())