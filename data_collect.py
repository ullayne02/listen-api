from const import ACCESS_TOKEN, CLIENT_ID,CLIENT_SECRET, REDIRECT_URL
import requests
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import json
import os

def GetPath(): 
    today = pd.Timestamp('today')
    return 'listen_music_{:%m%d%Y}'.format(today)

def save_daily(df, folder="data"):
    path = f"{folder}/{GetPath()}.parquet"

    if os.path.exists(path):
        print(f"{path} already exists, concatenating the data ...")
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, df]).drop_duplicates(['played_at'])
        combined.to_parquet(path, index=False)
    else:
        print(f"{path} being created..")
        df.to_parquet(path, index=False)

    print(f"Saved to {path}")

def get_spotify_50():
    print("Accesing spotify")
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URL,
        scope="user-read-recently-played"   # <-- this is what was missing
    ))
    results = sp.current_user_recently_played(limit=50)
    result_df = pd.json_normalize(results['items'])
    normalized_df = normalize_df(result_df)
    
    print(f"Successfly retrieved 50 songs listened today {pd.Timestamp('today')}")
    return normalized_df

def normalize_df(df): 
    normalized_df = df[["played_at", "track.album.id", "track.duration_ms", "track.id", "track.type"]]
    normalized_df["played_at"] = pd.to_datetime(normalized_df["played_at"])
    return normalized_df

def main(): 
    print("Starting data collect job....")

    df = get_spotify_50()
    save_daily(df)

    print("Finalizing spotify data collection script")

main()