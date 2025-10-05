import configparser
import os
import upstox_client
from upstox_client.rest import ApiException

# --- Configuration ---
def get_config():
    """
    Reads and returns configuration from my.properties, robustly finding the file.
    """
    config = configparser.ConfigParser()

    # Get the directory where this script (UpstoxLOGIN.py) is located.
    # This makes the path finding independent of the current working directory.
    current_dir = os.path.dirname(os.path.abspath(__file__))
    properties_path = os.path.join(current_dir, 'my.properties')

    if not os.path.exists(properties_path):
        raise FileNotFoundError(f"Configuration file not found at: {properties_path}")

    config.read(properties_path)
    return config

def get_login_url():
    """Constructs the Upstox login URL."""
    config = get_config()
    client_id = config['DEFAULT']['apikey']
    redirect_uri = config['DEFAULT']['redirect_uri']

    return f"https://api-v2.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"

def login_to_upstox(code):
    """
    Exchanges the authorization code for an access token and stores it.
    """
    config = get_config()
    client_id = config['DEFAULT']['apikey']
    client_secret = config['DEFAULT']['secret']
    redirect_uri = config['DEFAULT']['redirect_uri']

    api_instance = upstox_client.LoginApi()
    api_version = '2.0'
    grant_type = 'authorization_code'

    try:
        # Get token API
        api_response = api_instance.token(
            api_version,
            code=code,
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            grant_type=grant_type
        )

        access_token = api_response.access_token
        print("Successfully obtained access token.")

        # Store the access token in an environment variable
        os.environ['UPSTOX_ACCESS_TOKEN'] = access_token
        print("UPSTOX_ACCESS_TOKEN has been set in the environment.")

        return access_token

    except ApiException as e:
        print(f"Exception when calling LoginApi->token: {e}")
        return None