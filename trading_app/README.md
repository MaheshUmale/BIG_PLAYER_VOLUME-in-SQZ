# SQZ + Big Player Volume (BPV) Trading Application

This project is a fully integrated trading application that combines a multi-timeframe market scanner, a real-time data ingestion service, and a web-based charting UI. It is designed to help traders identify squeeze breakout opportunities and visualize them with live market data and Big Player Volume (BPV) indicators.

## Architecture

The application is composed of three main components that work together:

1.  **Scanner Service (`scanner` module):**
    *   Uses the original logic from the `BREAKOUT_SCANNER_WEBAPP`, leveraging `tradingview_screener` and `rookiepy` to find stocks in a multi-timeframe squeeze.
    *   Runs periodically in the background to detect new breakouts ("fired" events).
    *   Caches a list of stocks currently "in-progress" for the dashboard.
    *   Alerts the Pub/Sub service when a new breakout is detected.

2.  **Pub/Sub & Data Ingestion Service (`pubsub` module):**
    *   Receives alerts from the scanner for newly fired breakouts.
    *   Connects to the Upstox WebSocket API to subscribe to real-time tick data for the alerted symbols.
    *   Aggregates the tick data into 1-minute OHLC candles.
    *   Persists the candle data to a Google Firestore database in real-time.
    *   Includes a history fetcher to backfill data gaps.

3.  **Web Application (`webapp` module & Flask server):**
    *   A Flask web server that provides a REST API to serve the scanner data.
    *   **Scanner Dashboard (`/`):** The main landing page that displays two tables: "Squeezes in Progress" and "Breakouts Fired Today." This allows for high-level market analysis.
    *   **Charting Page (`/chart`):** A detailed, real-time charting UI that visualizes candlestick data and BPV bubbles for a single selected symbol.

## Setup and Installation

### Prerequisites

*   Python 3.10+
*   A Google Cloud project with Firestore enabled.
*   An Upstox developer account.
*   Brave Browser (or another browser supported by `rookiepy`) with a logged-in TradingView session.

### 1. Install Dependencies

Clone the repository and install the required Python packages:

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create a `.env` file in the root of the `trading_app` directory or set the following environment variables:

*   `FIREBASE_CONFIG`: The absolute path to your Google Cloud service account JSON file.
*   `UPSTOX_ACCESS_TOKEN`: Your valid access token for the Upstox API.
*   `APP_ID` (optional): A unique name for your application instance (e.g., `my-trading-app`).

### 3. TradingView Cookies (`rookiepy`)

The scanner depends on `rookiepy` to access your browser's cookies to authenticate with TradingView. Ensure you are logged into your TradingView account in the browser specified in `trading_app/scanner/original_scanner.py` (defaults to Brave).

If you use a different browser, you may need to modify this line:
```python
# In trading_app/scanner/original_scanner.py
cookies = rookiepy.to_cookiejar(rookiepy.brave(['.tradingview.com']))
```

## How to Run the Application

Once the setup is complete, you can run the entire application with a single command from the project's root directory:

```bash
python -m trading_app.main
```

This will:
1.  Initialize the Firebase and Upstox services.
2.  Start the WebSocket data handler in the background.
3.  Start the scanner loop in the background.
4.  Start the Flask web server.

You can then access the **Scanner Dashboard** by navigating to `http://127.0.0.1:8080` in your web browser. From the dashboard, you can click on any symbol to open its detailed chart in a new tab.