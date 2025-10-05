import asyncio
import os
import time
from threading import Thread

from trading_app.core.firebase_setup import initialize_firebase
from trading_app.scanner.original_scanner import run_scan, scanner_settings, init_db as init_scanner_db
from trading_app.pubsub.wss_handler import start_wss_connection

def main():
    """
    Main function to initialize and run the trading application.
    """
    # --- 1. Initialize Services ---
    try:
        print("Initializing services...")
        # Make sure environment variables are set before running
        db, auth, app_id, user_id = initialize_firebase()
        UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")

        if not UPSTOX_ACCESS_TOKEN:
            raise ValueError("UPSTOX_ACCESS_TOKEN environment variable not set. Cannot start real-time service.")

        # Initialize the scanner's SQLite database
        init_scanner_db()
        print("Services initialized successfully.")

    except (ValueError, FileNotFoundError) as e:
        print(f"FATAL: Initialization failed. Please check your environment configuration. Error: {e}")
        return

    # --- 2. Start WebSocket Handler in a Background Thread ---
    print("Starting WebSocket handler...")

    # The wss_handler runs an async event loop. We need to run this in a separate thread.
    def run_async_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(start_wss_connection(UPSTOX_ACCESS_TOKEN, db, app_id))

    wss_thread = Thread(target=run_async_loop, daemon=True)
    wss_thread.start()
    print("WebSocket handler is running in the background.")

    # Give the WebSocket a moment to establish a connection before the first scan
    print("Waiting 5 seconds for WebSocket to connect...")
    time.sleep(5)

    # --- 3. Run the Scanner Periodically ---
    SCAN_INTERVAL_MINUTES = 5
    print(f"Scanner is configured to run every {SCAN_INTERVAL_MINUTES} minutes.")

    while True:
        try:
            print(f"--- [{time.ctime()}] --- Starting new scanner run...")
            # The run_scan function is synchronous and can be called directly
            run_scan(settings=scanner_settings, db=db, app_id=app_id)
            print(f"--- Scanner run finished. ---")

        except Exception as e:
            print(f"An error occurred during a scanner run: {e}")
            # Depending on the error, you might want to add more robust error handling
            # or a cool-down period.

        print(f"Waiting for {SCAN_INTERVAL_MINUTES} minutes until the next scan cycle...")
        time.sleep(SCAN_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    # To run this application:
    # 1. Ensure you have a service account key file for Firebase.
    # 2. Set the required environment variables:
    #    export FIREBASE_CONFIG="path/to/your/serviceAccountKey.json"
    #    export UPSTOX_ACCESS_TOKEN="your_upstox_api_access_token"
    #    export APP_ID="your_app_name" (optional, defaults to 'default_trading_app')
    # 3. Make sure you are logged into TradingView in your Brave browser so rookiepy can find the cookies.
    #    If you use a different browser, you will need to adjust original_scanner.py.
    main()