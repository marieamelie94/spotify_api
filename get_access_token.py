import requests
import base64

def get_app_access_token(client_id, client_secret):
    """
    Returns an access token to be used in the headers of GET API calls to
    the Get Category's Playlists and Get Playlist endpoints of Spotify.
    Docs to create an app found here: https://developer.spotify.com/documentation/web-api/concepts/apps
    Args:
    client_id: your Spotify's app client_id
    client_secret: your Spotif's client_secret
    Returns: an access token to be used in downstream API calls as Authorization headers
    """
    auth_credentials_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_credentials_string.encode('ascii')
    auth_base64 = base64.b64encode(auth_bytes)
    auth_str = auth_base64.decode('ascii')
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_headers = {
        'Authorization': f"Basic {auth_str}",
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    auth_params = {
        'grant_type': 'client_credentials'
    }
    auth_response = requests.post(
        url=auth_url,
        data=auth_params,
        headers=auth_headers
    )
    access_token = auth_response.json()['access_token']
    return access_token