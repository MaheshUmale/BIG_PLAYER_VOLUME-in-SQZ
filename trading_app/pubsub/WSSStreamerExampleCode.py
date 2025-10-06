import requests
import upstox_client
from upstox_client.rest import ApiException
import websocket
import ssl
import threading

# --- Your Credentials and Settings ---
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"  # Replace with your actual access token
API_KEY = "YOUR_API_KEY"            # Replace with your actual API key
INSTRUMENT_KEYS = ["NSE_EQ|INE020B01018", "NSE_EQ|INE467B01029"] # Example instrument keys
MODE = "full" # Options: "ltpc", "full_d5", "full_d30", "option_greeks"

# --- 1. Authorize WebSocket feed using V3 REST endpoint ---
def get_market_data_feed_authorize_url(access_token):
    """Fetches the authorized WebSocket URI from the Upstox V3 REST endpoint."""
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() # Raise an exception for bad status codes
        return response.json().get("data", {}).get("authorized_redirect_uri")
    except Exception as e:
        print(f"❌ Failed to fetch V3 WebSocket URL: {e}")
        return None

# --- 2. Create and manage the MarketDataStreamer ---
def main():
    # Get the one-time authorized WebSocket URL
    ws_url = get_market_data_feed_authorize_url(ACCESS_TOKEN)
    if not ws_url:
        print("Could not get a valid WebSocket URL. Exiting.")
        return

    print("✅ Authorized WebSocket URL fetched.")
    
    # Configure and create the MarketDataStreamerV3 client
    configuration = upstox_client.Configuration()
    configuration.access_token = ACCESS_TOKEN
    api_client = upstox_client.ApiClient(configuration)

    try:
        streamer = upstox_client.MarketDataStreamerV3(api_client, ws_url)
        print("MarketDataStreamerV3 instance created.")
    except ApiException as e:
        print(f"Exception when creating MarketDataStreamerV3: {e}")
        return

    # --- 3. Define event handlers for the WebSocket ---
    def on_open():
        """Called when the WebSocket connection is established."""
        print("✅ Connection opened. Subscribing to instruments...")
        streamer.subscribe(INSTRUMENT_KEYS, MODE)

    def on_message(message):
        """Called for every incoming market data message."""
        print("➡️ Received Message:")
        # The SDK decodes the Protobuf message automatically
        print(message)
        # Add your logic here to process the market data

    def on_error(error):
        """Called when a WebSocket error occurs."""
        print(f"🔴 WebSocket Error: {error}")

    def on_close():
        """Called when the WebSocket connection is closed."""
        print("❌ Connection closed.")

    # --- 4. Attach event handlers and connect ---
    streamer.on("open", on_open)
    streamer.on("message", on_message)
    streamer.on("error", on_error)
    streamer.on("close", on_close)

    # The streamer.connect() method runs the event loop in the current thread.
    # To avoid blocking, it's run in a separate thread.
    print("Attempting to connect to the WebSocket...")
    thread = threading.Thread(target=streamer.connect)
    thread.start()

    # Keep the main thread alive to receive data. 
    # You can add a loop or wait for the thread to finish.
    try:
        thread.join()
    except KeyboardInterrupt:
        print("Exiting application...")
        streamer.disconnect()
        thread.join(timeout=5)
        print("Disconnected.")

if __name__ == "__main__":
    main()
