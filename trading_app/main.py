import asyncio
import os
import time
from threading import Thread, Lock
import pandas as pd
from flask import Flask, jsonify, render_template, request, redirect

# Local imports
from trading_app.core.database import init_db as init_local_db, load_all_day_fired_events_from_db
from trading_app.scanner.original_scanner import run_scan, scanner_settings
from trading_app.pubsub.wss_handler import start_wss_connection
from trading_app.UpstoxLOGIN import login_to_upstox, get_login_url

# --- Global Variables ---
scanner_cache = {
    "in_progress": pd.DataFrame()
}
cache_lock = Lock()
wss_thread = None

# --- Flask Web Server Setup ---
app = Flask(__name__, template_folder='webapp')

@app.route('/')
def dashboard():
    """Serves the main scanner dashboard UI or a login prompt."""
    access_token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not access_token:
        login_url = get_login_url()
        # A more user-friendly login page
        return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Login Required</title>
            <style>
                body {{ font-family: sans-serif; text-align: center; padding-top: 50px; }}
                button {{ font-size: 16px; padding: 10px 20px; }}
            </style>
        </head>
        <body>
            <h1>Welcome to the Trading App</h1>
            <p>The scanner service is active in the background.</p>
            <p>To view real-time market data, please log in using your Upstox account.</p>
            <a href="{login_url}" target="_blank"><button>Login with Upstox</button></a>
            <p style="margin-top: 20px;">After logging in, please <a href="/">refresh this page</a>.</p>
        </body>
        </html>
        """
    return render_template('scanner_dashboard.html')

@app.route('/callback')
def callback():
    """
    Handles the callback from Upstox after successful login.
    This endpoint receives the authorization code, exchanges it for an access token,
    and starts the WebSocket service.
    """
    global wss_thread
    code = request.args.get('code')
    if not code:
        return "Error: Authorization code not found in callback.", 400

    access_token = login_to_upstox(code)
    if access_token:
        if wss_thread is None or not wss_thread.is_alive():
            print("Access token received. Starting WebSocket handler...")
            wss_thread = start_wss_thread(access_token)
            time.sleep(5)  # Give the connection a moment to establish

        # This JavaScript closes the login popup and reloads the parent page.
        return """
            <script>
                window.opener.location.reload();
                window.close();
            </script>
            <h1>Login Successful!</h1>
            <p>This window will close automatically.</p>
        """
    else:
        login_url = get_login_url()
        return f"Error: Failed to obtain access token. Please <a href='{login_url}'>try again</a>.", 500

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

def start_wss_thread(access_token):
    """Starts the WebSocket handler in a daemon thread."""
    def run_async_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(start_wss_connection(access_token))

    thread = Thread(target=run_async_loop, daemon=True)
    thread.start()
    print("WebSocket handler thread started.")
    return thread

def main():
    """
    Main function to initialize and run the trading application.
    """
    global wss_thread
    # --- 1. Initialize Database ---
    print("Initializing local database...")
    init_local_db()
    print("Database initialized.")

    # --- 2. Start Scanner Loop (runs independently) ---
    scanner_thread = Thread(target=run_scanner_loop, daemon=True)
    scanner_thread.start()
    print("Scanner loop is running in the background.")

    # --- 3. Check for Existing Token and Start WebSocket if Found ---
    access_token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if access_token:
        print("Found existing access token. Starting WebSocket handler...")
        wss_thread = start_wss_thread(access_token)
    else:
        login_url = get_login_url()
        print("-" * 60)
        print("UPSTOX_ACCESS_TOKEN not found.")
        print("The scanner will run, but real-time data feed will be disabled.")
        print(f"To enable real-time data, open this URL and log in:")
        print(login_url)
        print("-" * 60)

    # --- 4. Start Flask Web Server (Main Thread) ---
    print("Starting Flask web server on http://0.0.0.0:8080")
    app.run(host='0.0.0.0', port=8080, debug=False)

if __name__ == "__main__":
    main()