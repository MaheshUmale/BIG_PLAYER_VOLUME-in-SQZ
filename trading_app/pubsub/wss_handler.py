import asyncio
import json
import ssl
import websockets
import upstox_client
from google.protobuf.json_format import MessageToDict
import os
import websockets
import json
import ssl
import asyncio
import http.client  # Used to make HTTP requests
import urllib.parse # Used to encode URL parameters

import requests
import upstox_client
from upstox_client.rest import ApiException
import websocket
import ssl
import threading

# --- V3 API Changes ---
# 1. New Protobuf file for V3 data structure
# NOTE: You MUST generate this file from the latest Upstox V3 MarketDataFeed.proto
from . import MarketDataFeedV3_pb2 as pb 
# ----------------------

from trading_app.pubsub.data_handler import process_live_feed
from trading_app.core.instrument_loader import get_instrument_key

def decode_protobuf_message(buffer):
    """Decodes a protobuf message from the WebSocket using the V3 schema."""
    try:
        # 2. Updated Protobuf Message class
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

# --- 1. Authorize WebSocket feed using V3 REST endpoint ---
def get_market_data_feed_authorize_url(access_token):
    """Fetches the authorized WebSocket URI from the Upstox V3 REST endpoint."""
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Api-Version": "3.0" # Specify API version for the HTTP request
    }
    
    try:
        conn = http.client.HTTPSConnection("api.upstox.com")
       
        conn.request("GET", "/v3/feed/market-data-feed/authorize", headers=headers)
        response = conn.getresponse()
        response_data = response.read().decode('utf-8')
        conn.close()
        
        
        if response.status == 200:
            auth_response = json.loads(response_data)
            print(auth_response)
            wss_url = auth_response.get("data", {}).get("authorizedRedirectUri")
            print(  "++++++"*60)
            print(wss_url)
            print('--'*60)
            print(  "++++++"*60)
            if not wss_url:
                print("Error: Could not get authorizedRedirectUri from V3 authorize endpoint.")
                return False
            print(f"Obtained WSS URL from V3 authorize endpoint: {wss_url}")
            return wss_url
            
        else:
            print(f"Error authorizing V3 WebSocket feed: {response.status} - {response_data}")
            return False

    except Exception as e:
        print(f"Error making HTTP request to V3 authorize endpoint: {e}")
        return False
    
      
    
    
async def start_wss_connection(access_token):
    """Initializes and maintains the WebSocket connection using v3 API."""
    global WEBSOCKET_CLIENT, RECONNECT_ATTEMPTS

    # API_VERSION is no longer strictly required for the access token configuration.
					   
    configuration = upstox_client.Configuration()
    configuration.access_token = access_token

    # Use the appropriate V3 API client instance (assuming SDK support)
    # If using an older SDK, you might need to manually build the V3 REST URL.
    # The current use of WebsocketApi with the V3 method is a common pattern.
    api_instance = upstox_client.WebsocketApi(upstox_client.ApiClient(configuration))

    # Prepare V3 Authorization Header for the WSS connection
    auth_header = {"Authorization": f"Bearer {access_token}"}

    while RECONNECT_ATTEMPTS < MAX_RECONNECT_ATTEMPTS:
        try:
            print(f"Authorizing WebSocket feed using V3 REST endpoint... ")
            print('---'*60)
            # 3. Call the V3 Authorization method
            # NOTE: Upstox V3 authorization is done via a REST API call to get the WSS URL.
            # api_response = api_instance.get_market_data_feed_authorize() # The method name is correct for V3
            wss_url = get_market_data_feed_authorize_url(configuration.access_token )
            print('---'*60)
            print(f" DONE Authorizing WebSocket feed using V3 REST endpoint... {wss_url}")
            print('---'*60)
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            # 4. Connect to the V3 WSS URL with the Authorization Header
            
        # 4. Connect to the V3 WSS URL with the Authorization Header (using the same header as above)
        # Note: The 'additional_headers' are used when initiating the WebSocket handshake.
        # try:
            async with websockets.connect(wss_url, ssl=ssl_context, additional_headers=auth_header) as websocket:
                WEBSOCKET_CLIENT = websocket
                RECONNECT_ATTEMPTS = 0 # Reset on successful connection
                print("WebSocket V3 connection established. Listening for data...")

                # Resubscribe to any instruments that were active before disconnection
                if ACTIVE_SUBSCRIPTIONS:
                    print(f"Resubscribing to {len(ACTIVE_SUBSCRIPTIONS)} instruments...")

                    # 5. V3 Subscription Payload (format is the same, but modes are V3-specific)
                    # Note: V3 offers modes like 'full', 'full_d30' as well as 'ltpc'.
                    # Assuming ACTIVE_SUBSCRIPTIONS holds the instrument keys.
                    await WEBSOCKET_CLIENT.send(json.dumps({
                        "guid": "resub-guid",
                        "method": "sub",
                        "data": {"mode": "ltpc", "instrumentKeys": list(ACTIVE_SUBSCRIPTIONS.keys())} # Use .keys() if ACTIVE_SUBSCRIPTIONS is a dict
                    }))

                while True:
                    # V3 data is still binary (Protobuf), so await websocket.recv() is correct
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
        finally:
            WEBSOCKET_CLIENT = None
            print("Attempting to reconnect...")
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
        # V3 subscription payload (keeping 'ltpc' mode)
        subscription_message = {
            "guid": f"sub-{symbol}",
            "method": "sub",
            "data": {
                "mode": "ltpc",
                "instrumentKeys": [instrument_key]
            }
        }
        # V3 WebSocket expects binary (JSON encoded to bytes) for the subscription message
        await WEBSOCKET_CLIENT.send(json.dumps(subscription_message).encode('utf-8'))
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
