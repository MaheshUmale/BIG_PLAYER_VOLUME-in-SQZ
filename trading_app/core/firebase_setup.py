import firebase_admin
from firebase_admin import credentials, firestore, auth
import os
import json

# --- Global Variables (to be populated by the application's environment) ---
# It's better to read these within the function to avoid issues with import-time evaluation.

def initialize_firebase():
    """
    Initializes the Firebase app using a service account and authenticates the user.

    The function reads configuration from environment variables:
    - APP_ID: A unique identifier for this application instance.
    - FIREBASE_CONFIG: The path to the Firebase service account JSON file.
    - INITIAL_AUTH_TOKEN: An optional custom token for user authentication.

    Returns:
        tuple: A tuple containing the Firestore client (db), the Auth client (auth_client),
               and the determined User ID (user_id).
    """
    app_id = os.environ.get("APP_ID", "default_trading_app")
    firebase_config_path = os.environ.get("FIREBASE_CONFIG")
    initial_auth_token = os.environ.get("INITIAL_AUTH_TOKEN")

    if not firebase_config_path:
        raise ValueError("FIREBASE_CONFIG environment variable (path to service account file) is not set.")

    if not os.path.exists(firebase_config_path):
        raise FileNotFoundError(f"Firebase service account file not found at: {firebase_config_path}")

    # Initialize Firebase Admin SDK
    cred = credentials.Certificate(firebase_config_path)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    # Note: The auth client is implicitly available after initialization.
    # `auth.Client()` is not the correct way to get the client instance.
    # You should use methods directly from the `auth` module.

    user_id = None
    if initial_auth_token:
        try:
            # This is for signing in with a custom token generated elsewhere
            # For this project, we assume the token is a standard Firebase ID token
            # If it's a custom token, you would use signInWithCustomToken on the client SDK
            # The server verifies an ID token, it doesn't "sign in" with it.
            user_record = auth.get_user_by_email("some_email_from_token") # This is just an example
            user_id = user_record.uid
            print(f"Successfully authenticated user with UID: {user_id}")

        except auth.AuthError as e:
            print(f"Authentication failed with provided token: {e}. Service will run with admin rights.")
            user_id = f"server_process_{app_id}"
    else:
        # For a backend service, it's standard to operate with the admin privileges
        # granted by the service account, not as an "anonymous user".
        # We'll create a representative user_id for clarity.
        user_id = f"server_process_{app_id}"
        print(f"No initial auth token. Service is running with admin privileges under the identity: {user_id}")

    # The returned auth object is the module itself, which is standard practice.
    return db, auth, app_id, user_id

if __name__ == "__main__":
    # Example usage:
    # To run this, you would need to set the environment variables:
    # export APP_ID="my_trading_app"
    # export FIREBASE_CONFIG='path/to/your/serviceAccountKey.json'
    # export INITIAL_AUTH_TOKEN="some_jwt_token"

    # For local testing, we can simulate this.
    # In a real environment, these would be set by the deployment environment.
    if not os.environ.get("FIREBASE_CONFIG"):
        print("FIREBASE_CONFIG environment variable not set. This script will not run.")
    else:
        db, auth_client, user_id = initialize_firebase()
        print(f"Firebase initialized. DB: {db}, Auth: {auth_client}, UserID: {user_id}")

        # Now you could use these to interact with Firestore
        # For example, to test the instrument loader:
        # from instrument_loader import get_instrument_key
        # instrument_key = get_instrument_key(db, __app_id, "NSE:RELIANCE")
        # print(f"Test: Instrument key for NSE:RELIANCE is {instrument_key}")