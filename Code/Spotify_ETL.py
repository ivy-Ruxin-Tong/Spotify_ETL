import requests
import json
import sys
import pandas as pd
import numpy as np
import datetime
import awswrangler as wr
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from io import StringIO


class get_music:


    def __init__(self):
        self.received_response = self.get_response()
        self.connect_to_s3 = self.connect_to_s3()
   

    def get_response(self):
        spotify_user_id = 
        spotify_token = 
        headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {spotify_token}"}


        today  = datetime.datetime.now().replace(hour = 0, minute= 0, second= 0, microsecond= 0)
        #yesterday = (today - datetime.timedelta(days = 1)).replace(hour = 0, minute= 0, second= 0, microsecond= 0)
        three_month_ago = (today - datetime.timedelta(days = 90)).replace(hour = 0, minute= 0, second= 0, microsecond= 0)
        three_month_ago_unix_timestamp = int(three_month_ago.timestamp()) * 1000
        #print(today.timestamp())
        today_unix_timestamp = int(today.timestamp()) * 1000

        get_query = "https://api.spotify.com/v1/me/player/recently-played"
        params = {
            "limit": 50,
            # "after": today_unix_timestamp
            "after": three_month_ago_unix_timestamp
        # "before": today_unix_timestamp}
        }

        response = requests.get(get_query, headers=headers, params=params)
        response_json = response.json()
        if not response_json:
            print("no responses are returned by Spotify")
        return response_json


    def get_lists_of_songs(self) -> pd.DataFrame:
        data = self.received_response 
        lists_of_songs = []
        for song in data['items']:
            # print("/n/n")
            # print(song)
            song_id = song['track']['id']
            song_name = song['track']['name']
            song_duration = song['track']['duration_ms']
            song_popularity = song['track']['popularity']
            song_played_at = song['played_at']
            album_id = song['track']['album']['id']
            artist_id = song['track']['album']['artists'][0]['id']
            song_element = {
                "song_id" : song_id,
                "song_name" : song_name,
                "song_duration" : song_duration,
                "song_popularity" : song_popularity, 
                "song_played_at" : song_played_at,
                "album_id" : album_id,
                "artist_id" : artist_id }
            lists_of_songs.append(song_element)
        songs = pd.DataFrame(lists_of_songs)
        songs['unique_id'] = songs['song_id'] + songs['song_played_at']
        songs['song_played_at'] = pd.to_datetime(songs['song_played_at']).dt.date
        return songs
        #print(songs)

    def get_lists_of_artists(self) -> pd.DataFrame:
        data = self.received_response 
        lists_of_artists = []
        for song in data['items']:
            # print("/n/n")
            # print(song)
            artist_id = song['track']['artists'][0]['id']
            artist_name = song['track']['artists'][0]['name']
            artist_attribute = {
                "artist_id" : artist_id,
                "artist_name" : artist_name}
            lists_of_artists.append(artist_attribute)
        artists = pd.DataFrame(lists_of_artists)
        artists = artists.drop_duplicates(subset=['artist_id'])
        return artists
        #print(artists)

    def get_lists_of_albums(self) -> pd.DataFrame:
        data = self.received_response 
        lists_of_albums = []
        for song in data['items']:
            album_id = song['track']['album']['id']
            album_name = song['track']['album']['name']
            album_release_date = song['track']['album']['release_date']
            album_total_tracks = song['track']['album']['total_tracks']
            artist_attribute = {
                "album_id" : album_id,
                "album_name" : album_name,
                "album_release_date" : album_release_date,
                "album_total_tracks" : album_total_tracks}
            lists_of_albums.append(artist_attribute)
        albums = pd.DataFrame(lists_of_albums)
        albums = albums.drop_duplicates(subset=['album_id'])
        return albums
        #print(albums)

    def data_validation(self, df:pd.DataFrame) -> bool:
        #  checked songs_played at, should be unique
        if 'played_at' not in df.columns:
            pass
        else:
            if not df['played_at'].is_unique():
                raise Exception("Please check data extraction")
        # check for null values
        if df.isnull().values.any():
            raise Exception("There are null values in the dataset, please check")

        # check date
        if 'played_at' not in df.columns:
            pass
        else:
            today  = datetime.datetime.now().replace(hour = 0, minute= 0, second= 0, microsecond= 0)
            three_month_ago = (today - datetime.timedelta(days = 90)).replace(hour = 0, minute= 0, second= 0, microsecond= 0)

            played_timestamps = df["played_at"].tolist()
            for played_timestamp in played_timestamps:
                if datetime.datetime.strptime(played_timestamp, '%Y-%m-%d') < three_month_ago :
                    raise Exception("At least one song from the dataset was from 3 months ago")

        # check album dataset
        if 'album_total_tracks' not in df.columns:
            pass
        else:
            if df['album_total_tracks'].values.any() <=0:
                raise Exception ("please check data extraction")
        return df
        #print(df) 

    
    def connect_to_s3(self):
        client_s3 = boto3.client('s3',
        aws_access_key_id = ''
        aws_secret_access_key = ' ')
        return client_s3

    def load_song(self,songs):
        client = self.connect_to_s3
        file_name = "songs.csv"
        bucket_name = 'spotifydatav1'
        csv_buffer = StringIO()
        songs.to_csv(csv_buffer)
        try:
            client.put_object(
                ACL = 'private',
                Body = csv_buffer.getvalue(),
                Bucket = bucket_name,
                Key = file_name)
        except ClientError as e:
            print("Incorrect Credentials")
        print("Finish Loading Songs dataset")

    def load_album(self,albums):
        client = self.connect_to_s3
        file_name = "albums.csv"
        bucket_name = 'spotifydatav1'
        csv_buffer = StringIO()
        albums.to_csv(csv_buffer)
        try:
            client.put_object(
                ACL = 'private',
                Body = csv_buffer.getvalue(),
                Bucket = bucket_name,
                Key = file_name)
        except ClientError as e:
            print("Incorrect Credentials")
        print("Finish Loading Album dataset")

    def load_artist(self,artists):
        client = self.connect_to_s3
        file_name = "artists.csv"
        bucket_name = 'spotifydatav1'
        csv_buffer = StringIO()
        artists.to_csv(csv_buffer)
        try:
            client.put_object(
                ACL = 'private',
                Body = csv_buffer.getvalue(),
                Bucket = bucket_name,
                Key = file_name)
        except ClientError as e:
            print("Incorrect Credentials")
        print("Finish Loading artists dataset")

    def run_etl(self):
        self.load_album(self.data_validation(self.get_lists_of_albums()))
        self.load_song(self.data_validation(self.get_lists_of_songs()))
        self.load_artist(self.data_validation(self.get_lists_of_artists()))


if __name__ == '__main__':
    a = get_music()
    a.run_etl()
# a.data_validation(a.get_lists_of_artists())
# a.data_validation(a.get_lists_of_songs())
# a.data_validation(a.get_lists_of_albums())

