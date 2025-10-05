import asyncio
import os
import time
from threading import Thread, Lock
import pandas as pd
from flask import Flask, jsonify, render_template

# Local imports
from trading_app.core.database import init_db as init_local_db, load_all_day_fired_events_from_db
from trading_app.scanner.original_scanner import run_scan, scanner_settings
from trading_app.pubsub.wss_handler import start_wss_connection

# --- Global Cache for Scanner Data ---
scanner_cache = {
    "in_progress": pd.DataFrame()
}
cache_lock = Lock()

# --- Flask Web Server Setup ---
app = Flask(__name__, template_folder='webapp')

@app.route('/')
def dashboard():
    """Serves the main scanner dashboard UI."""
    return render_template('scanner_dashboard.html')

@app.route('/chart')
def chart():
    """Serves the detailed chart page."""
    return render_template('index.html')

@app.route('/api/squeezes/in-progress')
def get_squeezes_in_progress():
    """API endpoint to get stocks currently in a squeeze."""
    with cache_lock:
        df = scanner_cache["in_progress"].copy()
        if df.empty:
            return jsonify([])
        df.fillna('', inplace=True)
        cols_to_send = ['name', 'close', 'rvol', 'SqueezeCount', 'highest_tf', 'squeeze_strength', 'momentum']
        for col in cols_to_send:
            if col not in df.columns:
                df[col] = ''
        data = df[cols_to_send].to_dict(orient='records')
    return jsonify(data)

@app.route('/api/squeezes/fired-today')
def get_squeezes_fired_today():
    """API endpoint to get all stocks that fired a breakout today."""
    df = load_all_day_fired_events_from_db() # This now calls the function from database.py
    if df.empty:
        return jsonify([])
    df.fillna('', inplace=True)
    cols_to_send = ['ticker', 'fired_timestamp', 'fired_timeframe', 'momentum', 'squeeze_strength']
    for col in cols_to_send:
        if col not in df.columns:
            df[col] = ''
    data = df[cols_to_send].to_dict(orient='records')
    return jsonify(data)

def run_scanner_loop(): # Removed db, app_id
    """The main loop for running the scanner periodically."""
    SCAN_INTERVAL_MINUTES = 5
    print(f"Scanner is configured to run every {SCAN_INTERVAL_MINUTES} minutes.")

    while True:
        try:
            print(f"--- [{time.ctime()}] --- Starting new scanner run...")
            in_progress_df = run_scan(settings=scanner_settings) # Removed db, app_id

            with cache_lock:
                if in_progress_df is not None:
                    scanner_cache["in_progress"] = in_progress_df

            print(f"--- Scanner run finished. Updated cache with {len(in_progress_df) if in_progress_df is not None else 0} in-progress squeezes. ---")

        except Exception as e:
            print(f"An error occurred during a scanner run: {e}")

        print(f"Waiting for {SCAN_INTERVAL_MINUTES} minutes until the next scan cycle...")
        time.sleep(SCAN_INTERVAL_MINUTES * 60)

def main():
    """
    Main function to initialize and run the trading application.
    """
    # --- 1. Initialize Services ---
    try:
        print("Initializing services...")
        # Initialize the local SQLite database
        init_local_db()

        UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")
        if not UPSTOX_ACCESS_TOKEN:
            raise ValueError("UPSTOX_ACCESS_TOKEN environment variable not set. Cannot start real-time service.")

        print("Services initialized successfully.")

    except (ValueError, FileNotFoundError) as e:
        print(f"FATAL: Initialization failed. Please check your environment configuration. Error: {e}")
        return

    # --- 2. Start WebSocket Handler in a Background Thread ---
    print("Starting WebSocket handler...")
    def run_async_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        # The wss_handler will also need to be updated to remove db and app_id
        loop.run_until_complete(start_wss_connection(UPSTOX_ACCESS_TOKEN))

    wss_thread = Thread(target=run_async_loop, daemon=True)
    wss_thread.start()
    print("WebSocket handler is running in the background.")

    # Give the WebSocket a moment to establish a connection
    print("Waiting 5 seconds for WebSocket to connect...")
    time.sleep(5)

    # --- 3. Start Scanner Loop in a Background Thread ---
    scanner_thread = Thread(target=run_scanner_loop, daemon=True)
    scanner_thread.start()
    print("Scanner loop is running in the background.")

    # --- 4. Start Flask Web Server in the Main Thread ---
    print("Starting Flask web server on http://0.0.0.0:8080")
    # Use Gunicorn or waitress in a production environment
    app.run(host='0.0.0.0', port=8080, debug=False)


if __name__ == "__main__":
    # To run this application:
    # 1. Set the required environment variables:
    #    export UPSTOX_ACCESS_TOKEN="your_upstox_api_access_token"
    # 2. Make sure you are logged into TradingView in your Brave browser so rookiepy can find the cookies.
    #    If you use a different browser, you will need to adjust original_scanner.py.
    main()