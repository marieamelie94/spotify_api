import logging
import os
import pandas as pd
import requests
from get_access_token import get_app_access_token


# Please provide your Spotify API Credentials
os.environ['CLIENT_ID'] = ''
os.environ['CLIENT_SECRET'] = ''

def _store_csv_gz(df, file_path):
    logging.info(f'Storing {file_path} ...')
    return df.to_csv(file_path, compression='gzip', index=False)


def get_categories(category_id, base_url, offset, limit, headers):
    # Connects to Spotify API and returns a df with categories information
    url = base_url + f'browse/categories/{category_id}/playlists?offset={offset}&limit={limit}'

    logging.info(f'Connecting to Spotify ...')
    try:
        response = requests.get(url, headers=headers).json()
    except:
        logging.info(f'Error connecting to API: {response}')

    df_categories = pd.DataFrame(response['playlists']['items'])

    # Take advantage of Spotify providing the next page url
    while response['playlists']['next'] is not None:
        response = requests.get(response['playlists']['next'], headers=bearer_token_headers).json()
        df_categories = pd.concat([df_categories, pd.DataFrame(response['playlists']['items'])], ignore_index=True)

    return df_categories


def load_categories(df_categories):
    # Grab necessary information from the API response and stores it
    list_playlist_ids = df_categories['id'].to_list()
    df_categories['tracks_url'] = df_categories['tracks'].apply(lambda x: x['href'])
    df_categories['total_tracks'] = df_categories['tracks'].apply(lambda x: x['total'])
    df_category_playlists_records = df_categories[
        ['description', 'name', 'id', 'tracks_url', 'total_tracks', 'snapshot_id']]
    logging.info('Data obtained and dataset ready')

    # Store
    _store_csv_gz(df_category_playlists_records, 'datasets/category_playlists_records.csv')

    return list_playlist_ids


def get_playlists(list_playlist_ids, base_url, fields, headers):
    # Connects to Spotify API and returns a list of playlist's information
    list_playlists = list()
    logging.info(f'Connecting to Spotify ...')

    # Grab each playlist's information
    for i in range(len(list_playlist_ids)):
        url = base_url +f'playlists/{list_playlist_ids[i]}?fields={fields}'
        try:
            response = requests.get(url, headers=headers).json()
        except:
            logging.info(f'Error connecting to API: {response}')
        list_playlists.append(response)

    return list_playlists


def load_playlists(list_playlists):
    # Grab necessary information from the API response and stores it
    # Prepare lists of dictionaries with relevant information
    list_playlist_records, list_tracks_records, list_tracks_items, list_artists_tracks, list_artists_records = ([] for i in range(5))
    errors = 0

    # Grab potentially nested information per playlist
    for playlist in list_playlists:
        try:
            list_playlist_records.append(
                dict(
                    playist_id = playlist['id'],
                    followers = playlist['followers']['total']
                )
            )
            for track_item in playlist['tracks']['items']:
                list_tracks_items.append(
                    dict(
                        playlist_id = playlist['id'],
                        id = track_item['track']['id'],
                        added_at = track_item['added_at']
                    )
                )
                list_tracks_records.append(
                    dict(
                        id=track_item['track']['id'],
                        album_type=track_item['track']['album']['album_type'],
                        name=track_item['track']['name'],
                        popularity=track_item['track']['popularity'],
                        uri=track_item['track']['uri']
                    )
                )
                for artist in track_item['track']['artists']:
                    list_artists_tracks.append(
                        dict(
                            track_id=track_item['track']['id'],
                            artist_id=artist['id']
                        )
                    )
                    list_artists_records.append(
                        dict(
                            id=artist['id'],
                            name=artist['name']
                        )
                    )

        except Exception as err:
            errors +=1
            print(f"Unexpected {err=}, {type(err)=}")
            print(f'Error parsing response fields for a playlist')
            # For time constraints just continuing the flow in case of error
            continue

    logging.info(f'Could not parse {str(errors)} playlists')

    # Store
    _store_csv_gz(pd.DataFrame(list_playlist_records), 'datasets/playlists_records.csv')
    _store_csv_gz(pd.DataFrame(list_tracks_records), 'datasets/tracks_records.csv')
    _store_csv_gz(pd.DataFrame(list_tracks_items), 'datasets/tracks_items.csv')
    _store_csv_gz(pd.DataFrame(list_artists_tracks), 'datasets/artists_tracks.csv')
    _store_csv_gz(pd.DataFrame(list_artists_records), 'datasets/artists_records.csv')


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='[%Y-%m-%d %H:%M:%S]', level=logging.INFO)

    # Inputs
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')

    category_id = 'latin'
    base_url = 'https://api.spotify.com/v1/'
    offset = 0
    limit = 50
    # Filter only on the fields we are interested in
    playlist_fields = "id,followers.total,tracks.items(added_at,track(id,name,popularity,uri,album(album_type),artists(id,name)))"

    # Get access
    access_token = get_app_access_token(client_id, client_secret)
    bearer_token_headers = {
        'Authorization': f"Bearer {access_token}"
    }

    # Start the flows
    categories_df = get_categories(category_id, base_url, offset, limit, bearer_token_headers)
    list_of_playlists = load_categories(categories_df)
    logging.info(f'Retrieved {len(list_of_playlists)} playlists')
    list_playlists = get_playlists(list_of_playlists, base_url, playlist_fields, bearer_token_headers)
    load_playlists(list_playlists)

    logging.info('All done, please check the datasets in target directory')
