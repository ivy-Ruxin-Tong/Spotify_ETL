import requests
import json
import sys
import datetime
import pandas as pd
import numpy as np
import boto3
from botocore.exceptions import ClientError
from io import StringIO
import base64


class GetRecentlyPlayedSongs:
    
    def __init__(self, user_initials,client_user_id, client_secret, *args, **kwargs):
        self.user_initials = user_initials
        self.client_user_id = client_user_id 
        self.client_secret = client_secret
        self.scope = 'user-read-recently-played'
        self.client_creds = f"{self.client_user_id}:{self.client_secret}" 
        self.bucket_name = 'spotifymusicetl'
        self.request_url = "https://accounts.spotify.com/api/token"
        self.redirect_url = 'https://www.google.com/'
        self.authorization_url = "https://accounts.spotify.com/authorize?"
        self.today = datetime.datetime.today().strftime('%Y-%m-%d')

       

    def refresh_token(self):
        # step 3 : Refresh Tokens
        request_url = self.request_url
        redirect_url =self.redirect_url
        # refresh_token = (self.request_token())
        client_creds_b64 = base64.b64encode(self.client_creds.encode())
        refresh_token = 'AQBs5nX0s6-V_SJvccdgG1JnWsY0Q9SSOM76dGLJvQV2aiYkPqJpClT2o1Dc5QdBJeGJklz0USsvWTIHKoocYgJ2CL1j5zZGmVu_89GhlMrOkDoVXi1RMKmAuGNQOjubYPw'
        request_url = "https://accounts.spotify.com/api/token"
        
        request_data = {
                "grant_type" : "refresh_token",
                'refresh_token' : refresh_token
            }
        request_header = {
                    "Authorization" : f"Basic {client_creds_b64.decode()}",
                    "Content-Type" : "application/x-www-form-urlencoded"
            }

        refresh_requests = requests.post(request_url, data = request_data, headers = request_header)  
        if refresh_requests.status_code != 200:
             raise Exception ("invalid refresh request response")
        refresh_token = refresh_requests.json()['access_token']
        #print(refresh_token)
        return refresh_token
    
    def get_spotify_token(self):
        spotify_token = self.refresh_token()
        return spotify_token

# if we're going to use airflow to schedule weekly update, then we can just not refresh the token, just generate the new one
  
    def get_response(self):
        spotify_token = self.get_spotify_token()
        headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {spotify_token}"}

        today  = datetime.datetime.now().replace(hour = 0, minute= 0, second= 0, microsecond= 0)
        yesterday = (today - datetime.timedelta(days = 1)).replace(hour = 0, minute= 0, second= 0, microsecond= 0)
        # week_ago = (today - datetime.timedelta(days = 7)).replace(hour = 0, minute= 0, second= 0, microsecond= 0)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
        # week_ago_unix_timestamp = int(week_ago.timestamp()) * 1000
        #print(today.timestamp())
        today_unix_timestamp = int(today.timestamp()) * 1000

        get_query = "https://api.spotify.com/v1/me/player/recently-played"
        params = {
            "limit": 50,
            "after": yesterday_unix_timestamp}
            # "after": week_ago_unix_timestamp}
            #"before": today_unix_timestamp}

        response = requests.get(get_query, headers=headers, params=params)
        response_json = response.json()
        if not response_json :
            raise Exception("no responses are returned by Spotify")
        elif "error" in response_json:
            print(response_json)
        return response_json


    def get_lists_of_songs(self) -> pd.DataFrame:
        #data = self.received_response
        data = self.get_response() 
        lists_of_songs = []
        for song in data['items']:
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
        songs['user'] = self.user_initials
        return songs

    def get_lists_of_artists(self) -> pd.DataFrame:
        #data = self.received_response
        data = self.get_response() 
        lists_of_artists = []
        for song in data['items']:
            artist_id = song['track']['artists'][0]['id']
            artist_name = song['track']['artists'][0]['name']
            artist_type = song['track']['artists'][0]['type']
            artist_attribute = {
                "artist_id" : artist_id,
                "artist_name" : artist_name,
                "artist_type" : artist_type
                }
            lists_of_artists.append(artist_attribute)
        artists = pd.DataFrame(lists_of_artists)
        artists = artists.drop_duplicates(subset=['artist_id'])
        return artists

    def get_lists_of_albums(self) -> pd.DataFrame:
        #data = self.received_response
        data = self.get_response() 
        lists_of_albums = []
        for song in data['items']:
            album_id = song['track']['album']['id']
            album_name = song['track']['album']['name']
            album_type = song['track']['album']['type']
            album_release_date = song['track']['album']['release_date']
            album_total_tracks = song['track']['album']['total_tracks']
            album_attribute = {
                "album_id" : album_id,
                "album_name" : album_name,
                "album_type" : album_type,
                "album_release_date" : album_release_date,
                "album_total_tracks" : album_total_tracks}
            lists_of_albums.append(album_attribute)
        albums = pd.DataFrame(lists_of_albums)
        albums = albums.drop_duplicates(subset=['album_id'])
        return albums

    def get_lists_of_time(self) -> pd.DataFrame:
        #data = self.received_response
        data = self.get_response() 
        lists_of_timeinfo = []
        for song in data['items']:
            song_played_at = song['played_at']
            time_element = {
                "song_played_at" : song_played_at}
            lists_of_timeinfo.append(time_element)
        time = pd.DataFrame(lists_of_timeinfo)
        if not time.empty:
            time['song_played_date'] = pd.to_datetime(time['song_played_at']).dt.date
            time['month'] = pd.to_datetime(time['song_played_at']).dt.month
            time['day'] = pd.to_datetime(time['song_played_at']).dt.day
            time['week_number'] = pd.to_datetime(time['song_played_at']).dt.isocalendar().week 
            time['weekday'] = pd.to_datetime(time['song_played_at']).dt.dayofweek

        return time

    def data_validation(self, df:pd.DataFrame) -> bool:
        #  checked songs_played at, should be unique
        if 'song_played_at' not in df.columns:
            pass
        else:
            if not df['song_played_at'].is_unique:
                raise Exception("Please check data extraction")

        # check for null values
        if df.isnull().values.any():
            raise Exception("There are null values in the dataset, please check")

        # check date
        if 'song_played_date' not in df.columns:
            pass
        else:
            today  = datetime.datetime.now().replace(hour = 0, minute= 0, second= 0, microsecond= 0)
            # week_ago = (today - datetime.timedelta(days = 7)).replace(hour = 0, minute= 0, second= 0, microsecond= 0).strftime("%Y-%m-%d")
            # yesterday = (today - datetime.timedelta(days = 1)).replace(hour = 0, minute= 0, second= 0, microsecond= 0).strftime("%Y-%m-%d")
            yesterday = (today - datetime.timedelta(days = 1)).replace(hour = 0, minute= 0, second= 0, microsecond= 0).date()
            played_dates = df["song_played_date"].tolist()
            for played_date in played_dates:
                if played_date < yesterday :
                    raise Exception("At least one song from the dataset was from 7 days ago")

        # check album dataset
        if 'album_total_tracks' not in df.columns:
            pass
        else:
            if df['album_total_tracks'].values.any() <=0:
                raise Exception ("please check data extraction")
        return df

    
    def connect_to_s3(self):
        client_s3 = boto3.client('s3',
        aws_access_key_id = '',
        aws_secret_access_key = '')
        return client_s3

    def load_song_s3(self,songs):
        # client = self.connect_to_s3
        client = self.connect_to_s3()
        today = self.today
        folder = "songs"
        file_name = f"songs_{today}.csv"
        #bucket_name = 'spotifymusicetl'
        bucket_name = self.bucket_name
        csv_buffer = StringIO()
        songs.to_csv(csv_buffer, index = False)
        try:
            client.put_object(
                ACL = 'private',
                Body = csv_buffer.getvalue(),
                Bucket = bucket_name,
                Key =  f"{folder}/{file_name}")
        except ClientError as e:
            print("Incorrect Credentials")
        print("Finish Loading Songs dataset")

    def load_album_s3(self,albums):
        # client = self.connect_to_s3
        client = self.connect_to_s3()
        today = self.today
        folder = 'albums'
        file_name = f"albums_{today}.csv"
        #bucket_name = 'spotifymusicetl'
        bucket_name = self.bucket_name
        csv_buffer = StringIO()
        albums.to_csv(csv_buffer, index = False)
        try:
            client.put_object(
                ACL = 'private',
                Body = csv_buffer.getvalue(),
                Bucket = bucket_name,
                Key =  f"{folder}/{file_name}")
        except ClientError as e:
            print("Incorrect Credentials")
        print("Finish Loading Album dataset")

    def load_artist_s3(self,artists):
        # client = self.connect_to_s3
        client = self.connect_to_s3()
        today = self.today
        folder = 'artists'
        file_name = f"artists_{today}.csv"
        bucket_name = self.bucket_name
        csv_buffer = StringIO()
        artists.to_csv(csv_buffer, index = False)
        try:
            client.put_object(
                ACL = 'private',
                Body = csv_buffer.getvalue(),
                Bucket = bucket_name,
                Key = f"{folder}/{file_name}")
        except ClientError as e:
            print("Incorrect Credentials")
        print("Finish Loading Artists dataset")

    def load_time_s3(self,time):
        # client = self.connect_to_s3
        client = self.connect_to_s3()
        today = self.today
        folder = 'time'
        file_name = f"time_{today}.csv"
        bucket_name = self.bucket_name
        csv_buffer = StringIO()
        time.to_csv(csv_buffer, index = False)
        try:
            client.put_object(
                ACL = 'private',
                Body = csv_buffer.getvalue(),
                Bucket = bucket_name,
                Key = f"{folder}/{file_name}")
        except ClientError as e:
            print("Incorrect Credentials")
        print("Finish Loading Time dataset")

    def run_etl_s3(self):
        self.load_album_s3(self.data_validation(self.get_lists_of_albums()))
        self.load_artist_s3(self.data_validation(self.get_lists_of_artists()))
        self.load_time_s3(self.data_validation(self.get_lists_of_time()))
        self.load_song_s3(self.data_validation(self.get_lists_of_songs()))

 

#if '__name__' == '__main__':
a= GetRecentlyPlayedSongs(user_initials = 'IT', client_user_id = "e3c7f163ac5e4e4a9e6e6f6d8ebae7a0",
        client_secret = "628d5e2bf9e445069371115dc6080a0a")
a.run_etl_s3()
