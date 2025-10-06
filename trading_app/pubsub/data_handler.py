from datetime import datetime
from trading_app.core.database import save_candle_data
from trading_app.core.instrument_loader import get_symbol_by_key

# This dictionary will hold the state of the current 1-minute candle for each subscribed instrument.
CANDLE_STATE = {}

async def process_live_feed(feed, broadcast_callback):
    """
    Processes a single tick from the live feed, updates the candle state,
    stores the completed candle, and broadcasts it to clients.
    """
    global CANDLE_STATE

    try:
        instrument_key = list(feed.keys())[0]
        tick = feed[instrument_key]['ltpc']

        price = tick.get('ltp')
        quantity = tick.get('ltq')
        timestamp_ms = int(tick.get('ltt'))

        if not all([price, quantity, timestamp_ms]):
            print(f"Skipping incomplete tick for {instrument_key}")
            return

        dt_object = datetime.fromtimestamp(timestamp_ms / 1000)
        current_minute = dt_object.replace(second=0, microsecond=0)

        if instrument_key not in CANDLE_STATE:
            # First tick for this instrument, initialize the candle
            CANDLE_STATE[instrument_key] = {
                'timestamp': current_minute.isoformat(),
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': quantity,
            }
            return

        # Update the existing candle state
        state = CANDLE_STATE[instrument_key]
        state_minute = datetime.fromisoformat(state['timestamp'])

        if current_minute > state_minute:
            # A new minute has started, the previous candle is complete.
            completed_candle = state
            symbol = get_symbol_by_key(instrument_key)
            print(f"Completed 1-min candle for {symbol}: {completed_candle}")

            # 1. Save to the central database
            save_candle_data(instrument_key, completed_candle)

            # 2. Broadcast the completed candle to SUBSCRIBED clients
            # The message now includes the symbol for targeted broadcasting
            await broadcast_callback({
                "type": "live_candle",
                "symbol": symbol,
                "data": completed_candle
            })

            # 3. Start a new candle for the current minute
            CANDLE_STATE[instrument_key] = {
                'timestamp': current_minute.isoformat(),
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': quantity,
            }
        else:
            # Continue updating the current candle
            state['high'] = max(state['high'], price)
            state['low'] = min(state['low'], price)
            state['close'] = price
            state['volume'] += quantity

    except (KeyError, TypeError, ValueError) as e:
        print(f"Error processing live feed: {e}. Feed data: {feed}")
    except Exception as e:
        print(f"An unexpected error occurred in process_live_feed: {e}")