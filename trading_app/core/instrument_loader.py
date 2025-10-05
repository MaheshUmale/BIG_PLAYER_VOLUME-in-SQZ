import gzip
import json
import os
import pandas as pd

# --- Global Cache for Instruments ---
INSTRUMENT_DF = None
INSTRUMENT_MAP = {} # For quick symbol-to-key lookups

def _load_instruments_from_file(file_path="trading_app/core/NSE.json.gz"):
    """
    Loads and caches instrument data from the local gzipped JSON file.
    This function is intended for internal use and will be called automatically.
    """
    global INSTRUMENT_DF, INSTRUMENT_MAP
    if INSTRUMENT_DF is not None:
        return # Already loaded

    print(f"Loading and caching instruments from {file_path}...")
    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            instruments = json.load(f)

        # Filter for NSE EQ segment
        nse_eq_instruments = [
            inst for inst in instruments
            if inst.get("exchange") == "NSE" and inst.get("segment") == "NSE_EQ"
        ]

        INSTRUMENT_DF = pd.DataFrame(nse_eq_instruments)

        # Create a mapping for faster lookups
        for instrument in nse_eq_instruments:
            symbol = f"{instrument['exchange']}:{instrument['trading_symbol']}"
            INSTRUMENT_MAP[symbol] = instrument["instrument_key"]

        print(f"Successfully loaded and cached {len(INSTRUMENT_DF)} NSE equity instruments.")

    except FileNotFoundError:
        print(f"FATAL: Instrument file not found at {file_path}. The application cannot map symbols to keys.")
        raise
    except Exception as e:
        print(f"An error occurred while loading instruments: {e}")
        raise

def get_instrument_key(symbol):
    """
    Retrieves the instrument key for a given symbol from the local cache.

    Args:
        symbol (str): The symbol to look up (e.g., "NSE:RELIANCE").

    Returns:
        str: The instrument key, or None if not found.
    """
    global INSTRUMENT_MAP
    # Ensure instruments are loaded before trying to access them
    if not INSTRUMENT_MAP:
        _load_instruments_from_file()

    return INSTRUMENT_MAP.get(symbol)

def get_all_instrument_keys():
    """
    Returns a list of all cached instrument keys.
    """
    global INSTRUMENT_MAP
    if not INSTRUMENT_MAP:
        _load_instruments_from_file()

    return list(INSTRUMENT_MAP.values())

def get_symbol_by_key(instrument_key):
    """
    Retrieves the trading symbol for a given instrument key from the local cache.
    """
    global INSTRUMENT_MAP
    if not INSTRUMENT_MAP:
        _load_instruments_from_file()

    # This is inefficient, but will work for now. A reverse map could be created if performance is an issue.
    for symbol, key in INSTRUMENT_MAP.items():
        if key == instrument_key:
            return symbol
    return None


if __name__ == "__main__":
    # --- Example Usage & Testing ---
    print("Testing instrument loader...")

    # 1. Test getting a specific instrument key
    reliance_key = get_instrument_key("NSE:RELIANCE")
    print(f"Instrument key for NSE:RELIANCE: {reliance_key}")
    assert reliance_key is not None

    # 2. Test getting a non-existent key
    non_existent_key = get_instrument_key("NSE:FAKESYMBOL")
    print(f"Instrument key for NSE:FAKESYMBOL: {non_existent_key}")
    assert non_existent_key is None

    # 3. Test that the cache is loaded only once
    print("\nRequesting another key (should use cache)...")
    infy_key = get_instrument_key("NSE:INFY")
    print(f"Instrument key for NSE:INFY: {infy_key}")
    assert infy_key is not None

    # 4. Test getting all keys
    all_keys = get_all_instrument_keys()
    print(f"\nTotal instrument keys loaded: {len(all_keys)}")
    assert len(all_keys) > 0

    # 5. Test reverse lookup
    symbol = get_symbol_by_key(reliance_key)
    print(f"\nSymbol for key {reliance_key}: {symbol}")
    assert symbol == "NSE:RELIANCE"

    print("\nInstrument loader tests passed.")