import sqlite3
import pandas as pd
import os
from datetime import datetime

# --- Database Setup ---
DB_FILE = "trading_app_data.db"
DB_CONNECTION = None

def get_db_connection():
    """
    Establishes and returns a connection to the SQLite database.
    """
    global DB_CONNECTION
    if DB_CONNECTION is None:
        try:
            # Using detect_types to handle timestamp conversion
            DB_CONNECTION = sqlite3.connect(DB_FILE, check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES)
            DB_CONNECTION.row_factory = sqlite3.Row
            print(f"Successfully connected to database: {DB_FILE}")
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            raise
    return DB_CONNECTION

def init_db():
    """
    Initializes the database by creating all necessary tables.
    """
    print("Initializing database...")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # --- Fired Events Table ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fired_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                fired_timestamp TEXT NOT NULL,
                fired_timeframe TEXT NOT NULL,
                momentum REAL,
                squeeze_strength REAL,
                UNIQUE(ticker, fired_timestamp, fired_timeframe)
            )
        """)

        # --- Squeeze History Table ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS squeeze_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_timestamp TIMESTAMP NOT NULL,
                ticker TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                volatility REAL
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_scan_timestamp ON squeeze_history (scan_timestamp)')


        # --- 1-Minute Candles Table ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS candles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_key TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                UNIQUE(instrument_key, timestamp)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_candle_timestamp_key ON candles (instrument_key, timestamp)')

        conn.commit()
        print("Database initialized successfully. Tables 'fired_events', 'squeeze_history', and 'candles' are ready.")
    except sqlite3.Error as e:
        print(f"An error occurred during database initialization: {e}")
        raise

def save_fired_events_to_db(fired_events_df):
    """
    Saves a DataFrame of fired events to the database.
    """
    if fired_events_df.empty:
        return

    # Ensure columns match the table schema
    events_to_save = fired_events_df[[
        'ticker', 'fired_timestamp', 'fired_timeframe', 'momentum', 'squeeze_strength'
    ]].copy()

    try:
        conn = get_db_connection()
        # Use a temporary table and INSERT OR IGNORE to handle unique constraints gracefully
        events_to_save.to_sql('temp_fired_events', conn, if_exists='replace', index=False)
        sql = '''
            INSERT OR IGNORE INTO fired_events (ticker, fired_timestamp, fired_timeframe, momentum, squeeze_strength)
            SELECT ticker, fired_timestamp, fired_timeframe, momentum, squeeze_strength FROM temp_fired_events
        '''
        conn.execute(sql)
        conn.commit()
        print(f"Successfully saved {len(events_to_save)} fired events to the database.")
    except Exception as e:
        print(f"Failed to save fired events to database: {e}")


def load_all_day_fired_events_from_db():
    """
    Loads all fired events for the current day from the database into a pandas DataFrame.
    """
    try:
        conn = get_db_connection()
        query = "SELECT ticker, fired_timestamp, fired_timeframe, momentum, squeeze_strength FROM fired_events ORDER BY fired_timestamp DESC"
        df = pd.read_sql_query(query, conn)
        return df
    except sqlite3.Error as e:
        print(f"Failed to load fired events from database: {e}")
        return pd.DataFrame()

def save_current_squeeze_list_to_db(squeeze_records):
    """
    Saves the current list of stocks in a squeeze to the history table.
    """
    if not squeeze_records:
        return

    now = datetime.now()
    # Prepare data for insertion
    data_to_insert = [
        (now, r['ticker'], r['timeframe'], r['volatility'])
        for r in squeeze_records
    ]

    sql = ''' INSERT INTO squeeze_history(scan_timestamp, ticker, timeframe, volatility)
              VALUES(?,?,?,?) '''

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.executemany(sql, data_to_insert)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Failed to save squeeze history: {e}")

def load_previous_squeeze_list_from_db():
    """
    Loads the most recent list of squeezed stocks from the database.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Find the timestamp of the last scan
        cursor.execute('SELECT MAX(scan_timestamp) FROM squeeze_history')
        last_timestamp = cursor.fetchone()[0]
        if last_timestamp is None:
            return [] # No history yet

        # Retrieve all records from the last scan
        cursor.execute('SELECT ticker, timeframe, volatility FROM squeeze_history WHERE scan_timestamp = ?', (last_timestamp,))
        return [(row['ticker'], row['timeframe'], row['volatility']) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Failed to load previous squeeze list: {e}")
        return []

def save_candle_data(instrument_key, candle_data):
    """
    Saves a single 1-minute candle to the database.
    Uses INSERT OR REPLACE to prevent duplicates.
    """
    sql = ''' INSERT OR REPLACE INTO candles(instrument_key, timestamp, open, high, low, close, volume)
              VALUES(?,?,?,?,?,?,?) '''
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql, (
            instrument_key,
            candle_data['timestamp'],
            candle_data['open'],
            candle_data['high'],
            candle_data['low'],
            candle_data['close'],
            candle_data['volume']
        ))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Failed to save candle for {instrument_key}: {e}")

if __name__ == '__main__':
    print("Running tests for the updated database module...")
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Removed existing database file: {DB_FILE}")

    # 1. Initialize DB
    init_db()

    # 2. Test Squeeze History
    print("\n--- Testing Squeeze History ---")
    squeeze_list = [
        {'ticker': 'RELIANCE', 'timeframe': '15m', 'volatility': 0.8},
        {'ticker': 'INFY', 'timeframe': 'Daily', 'volatility': 1.2}
    ]
    save_current_squeeze_list_to_db(squeeze_list)
    print("Saved current squeeze list.")

    previous_squeezes = load_previous_squeeze_list_from_db()
    print(f"Loaded previous squeeze list: {previous_squeezes}")
    assert len(previous_squeezes) == 2
    assert previous_squeezes[0][0] == 'RELIANCE'

    # 3. Test Fired Events
    print("\n--- Testing Fired Events ---")
    fired_df = pd.DataFrame([{
        'ticker': 'TCS',
        'fired_timestamp': datetime.now().isoformat(),
        'fired_timeframe': '60m',
        'momentum': 1.5,
        'squeeze_strength': 2.1
    }])
    save_fired_events_to_db(fired_df)

    all_fired = load_all_day_fired_events_from_db()
    print("Loaded all fired events:")
    print(all_fired)
    assert not all_fired.empty
    assert all_fired.iloc[0]['ticker'] == 'TCS'

    # 4. Test Candle Data
    print("\n--- Testing Candle Data ---")
    candle = {
        'timestamp': datetime.now().replace(second=0, microsecond=0).isoformat(),
        'open': 100, 'high': 102, 'low': 99, 'close': 101, 'volume': 5000
    }
    save_candle_data('NSE_EQ_RELIANCE', candle)
    print("Saved a sample candle.")

    # Verify candle was saved
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM candles", conn)
    print("Loaded candles from DB:")
    print(df)
    assert not df.empty
    assert df.iloc[0]['open'] == 100

    print("\nDatabase module tests passed.")