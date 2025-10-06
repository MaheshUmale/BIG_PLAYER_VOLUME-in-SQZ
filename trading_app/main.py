import asyncio
import os
import time
import json
from threading import Thread, Lock
import pandas as pd
from flask import Flask, jsonify, render_template, request, redirect
import websockets
from websockets.server import serve
import queue

# Local imports
from trading_app.core.database import init_db as init_local_db, load_all_day_fired_events_from_db
from trading_app.scanner.original_scanner import run_scan, scanner_settings
from trading_app.pubsub.wss_handler import start_wss_connection
from trading_app.UpstoxLOGIN import login_to_upstox, get_login_url
from trading_app.core.instrument_loader import get_instrument_key
from trading_app.core.database import get_db_connection

# --- Global Variables ---
scanner_cache = {
    "in_progress": pd.DataFrame()
}
cache_lock = Lock()
wss_thread = None
subscription_queue = queue.Queue() # Thread-safe queue for subscriptions

# --- WebSocket Server Setup ---
WEBSOCKET_CLIENTS = {} # Changed to a dict to map clients to their subscriptions
clients_lock = Lock()

async def broadcast_message(message):
    """
    Broadcasts a WebSocket message. If the message contains a 'symbol', it's sent
    only to clients subscribed to that symbol. Otherwise, it's sent to all clients.
    """
    if not WEBSOCKET_CLIENTS:
        return

    symbol_to_broadcast = message.get("symbol")
    json_message = json.dumps(message)

    tasks = []
    with clients_lock:
        if symbol_to_broadcast:
            # Targeted broadcast for clients subscribed to the specific symbol
            for client, subscriptions in WEBSOCKET_CLIENTS.items():
                if symbol_to_broadcast in subscriptions:
                    tasks.append(client.send(json_message))
        else:
            # General broadcast to all clients
            for client in WEBSOCKET_CLIENTS.keys():
                tasks.append(client.send(json_message))

    if tasks:
        await asyncio.gather(*tasks)

async def websocket_handler(websocket, path):
    """Handles WebSocket connections and subscriptions."""
    global WEBSOCKET_CLIENTS

    print(f"New client connected. Total clients: {len(WEBSOCKET_CLIENTS) + 1}")

    try:
        with clients_lock:
            WEBSOCKET_CLIENTS[websocket] = set() # Store subscribed symbols per client

        async for message in websocket:
            data = json.loads(message)
            action = data.get('action')

            if action == 'subscribe' and 'symbol' in data:
                symbol = data['symbol']
                print(f"Client subscribed to symbol: {symbol}")

                with clients_lock:
                    WEBSOCKET_CLIENTS[websocket].add(symbol)

                # Put the subscription request into the queue for the Upstox handler
                subscription_queue.put(symbol)

                # Fetch and send historical data
                instrument_key = get_instrument_key(symbol)
                if instrument_key:
                    conn = get_db_connection()
                    query = "SELECT * FROM candles WHERE instrument_key = ? AND date(timestamp) = date('now', 'localtime') ORDER BY timestamp"
                    df = pd.read_sql_query(query, conn, params=(instrument_key,))
                    if not df.empty:
                        historical_data = df.to_dict(orient='records')
                        await websocket.send(json.dumps({
                            "type": "historical_data",
                            "data": historical_data
                        }))
                        print(f"Sent {len(historical_data)} historical candles to client for {symbol}")

    except websockets.exceptions.ConnectionClosed:
        print("Client connection closed.")
    finally:
        with clients_lock:
            if websocket in WEBSOCKET_CLIENTS:
                del WEBSOCKET_CLIENTS[websocket]
        print(f"Client disconnected. Total clients: {len(WEBSOCKET_CLIENTS)}")


def start_websocket_server():
    """Starts the WebSocket server in a separate thread."""
    async def run_server():
        async with serve(websocket_handler, "0.0.0.0", 8765):
            print("WebSocket server started on ws://0.0.0.0:8765")
            await asyncio.Future()

    def loop_in_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_server())

    thread = Thread(target=loop_in_thread, daemon=True)
    thread.start()
    print("WebSocket server thread started.")

# --- Flask Web Server Setup ---
app = Flask(__name__, template_folder='webapp')

@app.route('/')
def dashboard():
    access_token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not access_token:
        login_url = get_login_url()
        return f"""
        <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Login Required</title>
        <style>body {{ font-family: sans-serif; text-align: center; padding-top: 50px; }} button {{ font-size: 16px; padding: 10px 20px; }}</style>
        </head><body><h1>Welcome to the Trading App</h1><p>The scanner service is active in the background.</p>
        <p>To view real-time market data, please log in using your Upstox account.</p>
        <a href="{login_url}" target="_blank"><button>Login with Upstox</button></a>
        <p style="margin-top: 20px;">After logging in, please <a href="/">refresh this page</a>.</p>
        </body></html>
        """
    return render_template('scanner_dashboard.html')

@app.route('/callback')
def callback():
    global wss_thread
    code = request.args.get('code')
    if not code:
        return "Error: Authorization code not found in callback.", 400

    access_token = login_to_upstox(code)
    if access_token:
        if wss_thread is None or not wss_thread.is_alive():
            print("Access token received. Starting WebSocket handler...")
            wss_thread = start_wss_thread(access_token, broadcast_message, subscription_queue)
            time.sleep(5)
        return """
            <script>window.opener.location.reload(); window.close();</script>
            <h1>Login Successful!</h1><p>This window will close automatically.</p>
        """
    else:
        login_url = get_login_url()
        return f"Error: Failed to obtain access token. Please <a href='{login_url}'>try again</a>.", 500

@app.route('/chart')
def chart():
    return render_template('index.html')

@app.route('/api/squeezes/in-progress')
def get_squeezes_in_progress():
    with cache_lock:
        df = scanner_cache["in_progress"].copy()
    if df.empty: return jsonify([])
    df.fillna('', inplace=True)
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/squeezes/fired-today')
def get_squeezes_fired_today():
    df = load_all_day_fired_events_from_db()
    if df.empty: return jsonify([])
    df.fillna('', inplace=True)
    return jsonify(df.to_dict(orient='records'))

def run_scanner_loop():
    SCAN_INTERVAL_MINUTES = 5
    print(f"Scanner is configured to run every {SCAN_INTERVAL_MINUTES} minutes.")
    while True:
        try:
            print(f"--- [{time.ctime()}] --- Starting new scanner run...")
            in_progress_df = run_scan(settings=scanner_settings)
            with cache_lock:
                if in_progress_df is not None:
                    scanner_cache["in_progress"] = in_progress_df
            print(f"--- Scanner run finished. Updated cache with {len(in_progress_df) if in_progress_df is not None else 0} in-progress squeezes. ---")
        except Exception as e:
            print(f"An error occurred during a scanner run: {e}")
        print(f"Waiting for {SCAN_INTERVAL_MINUTES} minutes until the next scan cycle...")
        time.sleep(SCAN_INTERVAL_MINUTES * 60)

def start_wss_thread(access_token, broadcast_callback, sub_queue):
    """Starts the WebSocket handler in a daemon thread."""
    def run_async_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(start_wss_connection(access_token, broadcast_callback, sub_queue))

    thread = Thread(target=run_async_loop, daemon=True)
    thread.start()
    print("Upstox WebSocket handler thread started.")
    return thread

def main():
    global wss_thread
    print("Initializing local database...")
    init_local_db()
    print("Database initialized.")

    start_websocket_server()

    scanner_thread = Thread(target=run_scanner_loop, daemon=True)
    scanner_thread.start()
    print("Scanner loop is running in the background.")

    access_token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if access_token:
        print("Found existing access token. Starting Upstox WebSocket handler...")
        wss_thread = start_wss_thread(access_token, broadcast_message, subscription_queue)
    else:
        login_url = get_login_url()
        print("-" * 60)
        print("UPSTOX_ACCESS_TOKEN not found.")
        print("The scanner will run, but real-time data feed will be disabled.")
        print(f"To enable real-time data, open this URL and log in: {login_url}")
        print("-" * 60)

    print("Starting Flask web server on http://0.0.0.0:8080")
    app.run(host='0.0.0.0', port=8080, debug=False)

if __name__ == "__main__":
    main()