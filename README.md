**Project Goal:** **SQZ + Big Player Volume (BPV) Trading Setup & Live Charting Application**

The final solution must integrate three logical components: a real-time market scanner, a robust Pub/Sub data ingestion service, and a single-page charting web application that visualizes both live data and "Big Player Volume" signals.



The following repositories contain the existing code/logic that must be adapted, integrated, and enhanced for this project. JULES, please clone, analyze, and extract the necessary logic from these sources:"

Scanner Logic: https://github.com/MaheshUmale/BREAKOUT_SCANNER_WEBAPP

Pub/Sub & Data Handling: https://github.com/MaheshUmale/PubSub_withScanner

Charting Visualization: https://github.com/MaheshUmale/BubbleChartApp


### Phase 1: Core Data & Authentication Setup

**Task 1.1: Upstox Instrument Key Mapping Service**

The application needs to map human-readable symbols (like `NSE:RELIANCE`) to Upstox's required `instrument_key` for WSS subscription and data fetching.

1.  **Download & Parse:** Create a dedicated function (or initialization script) to download the compressed `NSE.json.gz` file from the provided Upstox URL, unzip it, and parse the JSON content.
2.  **Database Seeding:** Store the necessary mapping (`trading_symbol`, `exchange`, `segment` -> `instrument_key`, `exchange_token`) into a dedicated Firestore collection (e.g., `/artifacts/{appId}/public/data/upstox_instruments`). This must be done daily or upon startup if the existing map is older than 24 hours.
3.  **Mapping Function:** Implement a reliable function `getInstrumentKey(symbol)` that queries the Firestore mapping collection to retrieve the Upstox `instrument_key` for a given symbol (e.g., `NSE:RELIANCE`).

**Task 1.2: Firebase & Authentication Initialization**

1.  **Setup:** Initialize Firebase App, Firestore (`db`), and Authentication (`auth`) using the provided global variables (`__app_id`, `__firebase_config`, `__initial_auth_token`).
2.  **Authentication:** Ensure the user is authenticated using `signInWithCustomToken(__initial_auth_token)` or `signInAnonymously()` if the token is undefined. Store the resulting `userId`.

### Phase 2: Pub/Sub & Data Ingestion Backend Service

This service will handle WSS subscriptions, real-time data flow, historical data retrieval, and persistence, mirroring the logic of the `PubSub_withScanner` example. This service must be robust and run independently.

**Task 2.1: Data Structure and Persistence**

1.  **Schema:** Define a strict Firestore schema for storing real-time 1-minute candle data, partitioned by symbol and date (e.g., `/artifacts/{appId}/public/data/live_candles/{symbol}_{date}`). Data should be stored with high precision (timestamps, candle data, and the raw WSS `ltq` for volume analysis).
2.  **Retention:** Implement a cleanup mechanism (or enforce in read/write logic) to ensure data older than **7 days** is not fetched or is pruned periodically.
3.  **Real-time Storage:** Create a function `storeCandleData(symbol, candle)` that writes the latest 1-minute candle (and associated WSS metadata) to Firestore in real-time.

**Task 2.2: WSS Subscription and Pub/Sub Logic**

1.  **Subscription Management:** Create a primary function `subscribeToSymbol(symbol)` that first uses **Task 1.1** to get the `instrument_key`. If the symbol is not already active, it initiates an Upstox WSS connection/topic subscription for that instrument.
2.  **WSS Handler:** Implement the WebSocket handler logic to receive live feed data from Upstox. Convert raw WSS ticks into **1-minute OHLC candles** and immediately call **Task 2.1** to persist them.
3.  **Alert Endpoint:** Expose a simple API endpoint/function `handleScannerAlert(symbol)` that the Scanner (Phase 3) will call. This function will trigger `subscribeToSymbol(symbol)` to ensure a live feed is initiated for the newly detected breakout symbol.

**Task 2.3: Historical Data Filling (Gap Coverage)**

1.  **Intraday API Fetcher:** Implement a function `fillMissingIntradayData(symbol, start_time, end_time)` that uses the **Upstox Client Intraday 1-min API** to fetch data for any time gap.
    * *Requirement:* This is used if a symbol is subscribed *after* 9:15 AM to backfill data from 9:15 AM to the subscription time.
    * *Persistence:* The fetched historical data must also be stored using **Task 2.1**'s persistence logic.

### Phase 3: TradingView Scanner Service & Alerting

This service, based on the `BREAKOUT_SCANNER_WEBAPP` example, will find trading opportunities and alert the Pub/Sub Service.

**Task 3.1: Squeeze Breakout Detection Logic**

1.  **Scanner Implementation:** Implement the core logic for running the Squeeze and Squeeze Breakout scan (based on TradingView Screener or a simplified, local technical analysis equivalent).
2.  **Trigger:** The scan should run periodically (e.g., every 5 minutes during market hours).

**Task 3.2: Alert and Communication**

1.  **Alert Generation:** When a Squeeze or Squeeze Breakout is detected **TODAY**, format the alert as a full symbol string (e.g., `NSE:TATASTEEL`).
2.  **POST to Pub/Sub:** Immediately send this symbol to the Pub/Sub Service's alert endpoint (**Task 2.2**) to initiate the WSS subscription and data ingestion.

### Phase 4: Charting UI and Visualization Application

Create a single-file HTML/JavaScript application (using Tailwind CSS for styling) that serves as the user-facing chart, adapting the `BubbleChartApp` visualization.

**Task 4.1: URL Handling and Initial Load**

1.  **Single-Page App:** Create a visually appealing, single HTML file (fully responsive) that accepts a URL parameter: `?symbol=NSE:SYMBOL`.
2.  **UI Components:** Include a placeholder for the chart canvas, a "Loading" indicator, and a button/link to easily copy the current `userId` (mandatory for multi-user apps).
3.  **Data Initialization Sequence:** Upon loading, implement this sequential logic:
    a. Get the `symbol` from the URL.
    b. Check **Firestore (Task 2.1)** for all 1-minute data points available for the current symbol and today's date.
    c. Determine if the data starts at the market open (9:15 AM).
    d. **Gap Filling:** If data is missing (does not start at 9:15 AM), call the backend function **Task 2.3** (`fillMissingIntradayData`) to backfill the gap using the Upstox API.
    e. **Live Subscription:** Once initial data (from DB/Historical Fill) is loaded, connect to the Pub/Sub Service (**Task 2.2**) to start receiving and displaying the live WSS feed.

**Task 4.2: Candlestick Bubble Chart Visualization**

1.  **Chart Adaptation:** Implement the Candlestick chart visualization, adapting the logic from the `BubbleChartApp` to show OHLC data.
2.  **Big Player Volume (BPV) Bubbles:** On the same chart, overlay a scatter/bubble plot.
    * **Logic:** The size of the bubble must be directly proportional to the "large `ltq`" (Last Traded Quantity) spike associated with that 1-minute candle, effectively visualizing the "Big Player" positions.
    * **Aesthetics:** Ensure bubbles are visually distinct and clearly highlight moments of significant single-trade volume.
3.  **Real-time Update:** Ensure the chart dynamically updates every time new data is received from the live Pub/Sub feed.
