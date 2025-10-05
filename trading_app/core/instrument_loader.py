import requests
import gzip
import json
import os

def download_and_parse_instrument_file(url, output_path="."):
    """
    Downloads, decompresses, and parses the Upstox instrument file.

    Args:
        url (str): The URL to the gzipped instrument file.
        output_path (str): The directory to save the downloaded and parsed files.

    Returns:
        list: A list of instrument data.
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)

        gz_filename = os.path.join(output_path, "instruments.json.gz")
        json_filename = os.path.join(output_path, "instruments.json")

        # Download the file
        print(f"Downloading instrument file from {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes

        with open(gz_filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded to {gz_filename}")

        # Decompress the file
        print(f"Decompressing {gz_filename}...")
        with gzip.open(gz_filename, "rb") as f_in:
            with open(json_filename, "wb") as f_out:
                f_out.write(f_in.read())
        print(f"Decompressed to {json_filename}")

        # Parse the JSON file
        print(f"Parsing {json_filename}...")
        with open(json_filename, "r", encoding="utf-8") as f:
            instruments = json.load(f)
        print("Parsing complete.")

        return instruments

    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
        return None
    except gzip.BadGzipFile:
        print("Error: The downloaded file is not a valid gzip file.")
        return None
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the instrument file.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def seed_instruments_to_firestore(db, instruments, app_id):
    """
    Seeds the instrument data into Firestore.

    Args:
        db: The Firestore client.
        instruments (list): A list of instrument data.
        app_id (str): The application ID.
    """
    print("Seeding instruments to Firestore...")
    batch = db.batch()
    collection_ref = db.collection("artifacts", app_id, "public", "data", "upstox_instruments")

    for instrument in instruments:
        if instrument.get("exchange") == "NSE" and instrument.get("segment") == "EQ":
            symbol = f"{instrument['exchange']}:{instrument['trading_symbol']}"
            doc_ref = collection_ref.document(symbol)
            batch.set(doc_ref, {
                "instrument_key": instrument["instrument_key"],
                "exchange_token": instrument["exchange_token"],
                "trading_symbol": instrument["trading_symbol"],
                "exchange": instrument["exchange"],
                "segment": instrument["segment"],
            })

    batch.commit()
    print("Seeding complete.")

def get_instrument_key(db, app_id, symbol):
    """
    Retrieves the instrument key for a given symbol from Firestore.

    Args:
        db: The Firestore client.
        app_id (str): The application ID.
        symbol (str): The symbol to look up (e.g., "NSE:RELIANCE").

    Returns:
        str: The instrument key, or None if not found.
    """
    doc_ref = db.collection("artifacts", app_id, "public", "data", "upstox_instruments").document(symbol)
    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict().get("instrument_key")
    return None

if __name__ == "__main__":
    # For local development, we'll use the copied NSE.json.gz file.
    # In a real application, you would implement the download logic.

    # For now, let's just focus on parsing the local file.
    local_gz_path = "trading_app/core/NSE.json.gz"
    json_filename = "trading_app/core/instruments.json"

    try:
        print(f"Decompressing {local_gz_path}...")
        with gzip.open(local_gz_path, "rb") as f_in:
            with open(json_filename, "wb") as f_out:
                f_out.write(f_in.read())
        print(f"Decompressed to {json_filename}")

        print(f"Parsing {json_filename}...")
        with open(json_filename, "r", encoding="utf-8") as f:
            instruments_data = json.load(f)
        print("Parsing complete.")

        # The rest of the logic (seeding to Firestore) will be called from the main application
        # after Firebase is initialized.

        # Example of how it would be used:
        # from firebase_setup import initialize_firebase
        # db, auth, app_id = initialize_firebase()
        # seed_instruments_to_firestore(db, instruments_data, app_id)
        # instrument_key = get_instrument_key(db, app_id, "NSE:RELIANCE")
        # print(f"Instrument key for NSE:RELIANCE is {instrument_key}")

    except FileNotFoundError:
        print(f"Error: {local_gz_path} not found. Please ensure the file is in the correct directory.")
    except Exception as e:
        print(f"An error occurred: {e}")