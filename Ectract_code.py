import json
from datetime import datetime

def lambda_handler(event, context):
    ## Extract Function

    import boto3
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    
    ## Connect with Spotify :
    auth_manager = SpotifyClientCredentials(client_id="148edc6a471a4f78890055f7d5f6149d",client_secret="f38ac5f5aedd4c039d9dd2bd163ec798")
    sp = spotipy.Spotify(auth_manager=auth_manager)
    
    ## Enter the name of the playlist and get json data:
    playlist_code = 'https://open.spotify.com/playlist/5ABHKGoOzxkaa28ttQV9sE?si=2cb9e4e2e87943b2'
    playlist_dict = sp.playlist(playlist_code)
    playlist_name  = playlist_dict['name']
    
    ## AWS credentials
    aws_access_key_id = 'AKIA2U3TXUVMHXFUQGMC'
    aws_secret_access_key = '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'
    
    ## Create boto client to access aws s3 service
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    #Declare necessary variables:
    file_name =f"{playlist_name}-{datetime.now().strftime('%Y-%m-%d')}.json"
    bucket_name = "my-bucket-28may"
    
    ## Write json file in s3 folder:
    json_data = json.dumps(playlist_dict)
    s3.put_object(Body=json_data, Bucket=bucket_name, Key= "raw_data/" + file_name)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
