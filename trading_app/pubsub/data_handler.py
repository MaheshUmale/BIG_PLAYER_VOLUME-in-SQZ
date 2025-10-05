from google.cloud.firestore_v1.base_query import FieldFilter
from datetime import datetime, timedelta

def store_candle_data(db, app_id, symbol, candle):
    """
    Stores a 1-minute candle data point into Firestore.

    The data is stored in a document representing the current day for the symbol.
    Each new candle is appended to an array within that document.

    Args:
        db: The Firestore client.
        app_id (str): The application ID.
        symbol (str): The trading symbol (e.g., "NSE:RELIANCE").
        candle (dict): A dictionary containing the candle data (timestamp, open, high, low, close, volume, ltq).
    """
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    doc_id = f"{symbol}_{today_str}"

    doc_ref = db.collection("artifacts", app_id, "public", "data", "live_candles").document(doc_id)

    try:
        # Atomically add the new candle to the 'candles' array field.
        # If the document or field does not exist, it's created.
        doc_ref.update({
            'candles': firestore.ArrayUnion([candle])
        })
        print(f"Stored candle for {symbol} at {candle['timestamp']}")
    except Exception as e:
        # If the document doesn't exist, update will fail. We need to create it.
        if 'not found' in str(e).lower():
            doc_ref.set({
                'symbol': symbol,
                'date': today_str,
                'candles': [candle]
            })
            print(f"Created new daily document and stored first candle for {symbol}")
        else:
            print(f"Error storing candle data for {symbol}: {e}")


def get_daily_candles(db, app_id, symbol, date_str):
    """
    Retrieves all 1-minute candles for a given symbol and date.

    Args:
        db: The Firestore client.
        app_id (str): The application ID.
        symbol (str): The trading symbol.
        date_str (str): The date in 'YYYY-MM-DD' format.

    Returns:
        list: A list of candle data dictionaries, or an empty list if not found.
    """
    doc_id = f"{symbol}_{date_str}"
    doc_ref = db.collection("artifacts", app_id, "public", "data", "live_candles").document(doc_id)

    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict().get("candles", [])
    return []


def prune_old_data(db, app_id, days_to_keep=7):
    """
    Deletes candle data older than a specified number of days.

    Args:
        db: The Firestore client.
        app_id (str): The application ID.
        days_to_keep (int): The number of days of data to retain.
    """
    cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
    cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')

    collection_ref = db.collection("artifacts", app_id, "public", "data", "live_candles")

    # Firestore does not support inequality filters on document IDs directly in this manner.
    # We must filter by a field in the document.
    query = collection_ref.where(filter=FieldFilter('date', '<', cutoff_date_str))

    docs_to_delete = query.stream()

    deleted_count = 0
    for doc in docs_to_delete:
        print(f"Deleting old data: {doc.id}")
        doc.reference.delete()
        deleted_count += 1

    print(f"Pruned {deleted_count} old document(s).")


if __name__ == '__main__':
    # This block is for demonstration and requires Firebase to be initialized.
    # from trading_app.core.firebase_setup import initialize_firebase

    # # Assume environment variables are set for initialize_firebase()
    # try:
    #     db, auth, app_id, user_id = initialize_firebase()

    #     # --- Test storing data ---
    #     test_symbol = "NSE:TESTSTOCK"
    #     test_candle = {
    #         'timestamp': datetime.utcnow().isoformat(),
    #         'open': 100.5,
    #         'high': 102.1,
    #         'low': 99.8,
    #         'close': 101.2,
    #         'volume': 5000,
    #         'ltq': 100
    #     }
    #     store_candle_data(db, app_id, test_symbol, test_candle)

    #     # --- Test retrieving data ---
    #     today = datetime.utcnow().strftime('%Y-%m-%d')
    #     candles = get_daily_candles(db, app_id, test_symbol, today)
    #     print(f"Retrieved {len(candles)} candles for {test_symbol} on {today}.")
    #     if candles:
    #         print("First candle:", candles[0])

    #     # --- Test pruning old data ---
    #     # To test this, you would need to manually create some old data in Firestore.
    #     # For example, a document with a 'date' field of "2023-01-01".
    #     prune_old_data(db, app_id, days_to_keep=7)

    # except (ValueError, FileNotFoundError) as e:
    #     print(f"Firebase initialization failed. Please set environment variables. Error: {e}")
    pass