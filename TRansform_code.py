import json
from datetime import datetime
import time

def lambda_handler(event, context):
    ## Transform function
    #import json
    import pandas as pd
    #from datetime import datetime
    import boto3
    # with open('D://spotify.json') as json_file:
    #     data = json.load(json_file)
        
    bucket = 'my-bucket-28may' # already created on S3
    #s3_client = boto3.client('s3')
    folder = 'raw_data'
    file = 'Top 100 most streamed songs on Spotify *Updated*-2023-06-02.json'
    #file = f"Top 100 most streamed songs on Spotify *Updated*-{datetime.now().strftime('%Y-%m-%d')}.json"
    
    #response = s3_client.list_objects_v2(Bucket=bucket, Prefix = folder)
    #file_object = response['Contents'][0]  # Assuming there's only one object in the bucket
    # # print(response)
    #file_key = response['Contents'][0]['Key']
    #response_file = s3_client.get_object(Bucket=bucket, Key=file_key)
    #json_data = response_file['Body'].read().decode('utf-8')
    
    s3 =boto3.resource('s3')
    obj = s3.Object(bucket, f'{folder}/{file}')
    json_data = obj.get()['Body'].read().decode('utf-8')
    
    # Parse the JSON data
    data = json.loads(json_data)
    # print(json_data)
    tracks = data['tracks']['items']
    total = data['tracks']['total']
    
    
    song_dict = {'song_name':[],'Track_popularity':[],'artist_name':[],'song_duration':[],'Track_number':[],'add_date':[]}
    
    for i in range(0, total):
        song_dict['song_name'].append(tracks[i]['track']['name'])
        # dict['album_name'].append(tracks[i]['track']['album']['name'])
        song_dict['Track_popularity'].append(tracks[i]['track']['popularity'])
        song_dict['artist_name'].append(tracks[i]['track']['album']['artists'][0]['name'])
        song_dict['song_duration'].append(tracks[i]['track']['duration_ms'])
        song_dict['Track_number'].append(tracks[i]['track']['popularity'])
        song_dict['add_date'].append(tracks[i]['added_at'])
    
    song_df = pd.DataFrame.from_dict(song_dict)
    
    ## Let's check if there's any null data and and if so, remove it:
    for column in song_df.columns:
        if song_df[f'{column}'].isnull().sum() > 0:
            song_df.dropna(subset=[column], inplace = True)
            
    ##Below I have provided 2 formats minutes::seconds and hours::minutes::seconds you can use any one of it :-
    
    ## Create a function to convert the seconds to minutes::seconds format:- 
    def convert(seconds):
        min, sec = divmod(seconds, 60)
        return '%02d:%02d' % (min, sec)
    
    ## Create a function to convert the seconds to hours::minutes::seconds format:- 
    # import datetime
    # def convert(seconds):
    #     return str(datetime.timedelta(seconds=seconds))
            
            
    ## Convert the 'song_duration' from milli-seconds to seconds and Round tham for integer values :
    song_df['song_duration'] = round(song_df['song_duration']/1000).apply(convert)
    song_df['add_date'] = pd.to_datetime(song_df['add_date'])
    ## Load program 
    
    ## Write this song_df dataframe to s3:-
    from io import StringIO # python3; python2: BytesIO 
    import boto3

    
    
    ## StringIO allows us to create in-memory file-like object. 
    ## This helps us to convert dataframe to csv format without writing it to a physical file.
    
    csv_buffer = StringIO() ## used to store csv data temporarily.
    song_df.to_csv(csv_buffer)
    body = csv_buffer.getvalue() # gets all the data 
    s3_resource = boto3.resource('s3')
    folder1 = 'songs/'
    s3_resource.Object(bucket, folder1 + f"Song-{datetime.now().strftime('%Y-%m-%d')}-songs.csv").put(Body=body)
    
    
    ## Let's create Album_dictionary from it and transform it in similar manner :-     
    album_dict = {'Album_name':[],'Album_artist':[],'Album_total_tracks':[],'Album_release_date':[],'Track_popularity':[]}
    
    for j in range(0,total):
        album_dict['Album_name'].append(tracks[j]['track']['album']['name'])
        album_dict['Album_artist'].append(tracks[j]['track']['album']['artists'][0]['name'])
        album_dict['Album_total_tracks'].append(tracks[j]['track']['album']['total_tracks'])
        album_dict['Album_release_date'].append(tracks[j]['track']['album']['release_date'])
        album_dict['Track_popularity'].append(tracks[j]['track']['popularity'])
        
    album_df = pd.DataFrame.from_dict(album_dict)
    
    ## Let's check if there's any null data and and if so, remove it:
    for column in album_df.columns:
        if album_df[column].isnull().sum() > 0:
            album_df.dropna(subset=[column],inplace=True)
    
            
    ## Load program 
    ## Store this album_df to s3 in csv format :- 
    
    csv_buffer = StringIO() ## used to store csv data temporarily.
    album_df.to_csv(csv_buffer)
    body = csv_buffer.getvalue() # gets all the data 
    folder2 = 'albums/'
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, folder2 + f"Album-{datetime.now().strftime('%Y-%m-%d')}-.csv").put(Body=body)
    
    bucket = 'my-bucket-28may'
    source_folder = 'raw_data'
    destination_folder = 'stored_raw_data'
    
    
    ## Create s3 client to access the s3_bucket:
    s3_client = boto3.client('s3')
    
    ## There's only one file in source folder and we don't know file_name
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=source_folder)
    file_key = response['Contents'][0]['Key']
    ## Extract the file_name:-
    file_name = file_key.split('/')[-1]
    #print(file_name)
    
    s3_resource = boto3.resource('s3')
    #Copy the file to the destination folder
    try:
        #copy_source = {'Bucket': bucket, 'Key':file_key}
        # s3_client.copy_object(CopySource =copy_source, Bucket = bucket, Key=destination_folder + file_name)
        #s3_resource.Object(bucket, f"{destination_folder}/{file}").copy_from(CopySource=copy_source)
        my_key=f"{source_folder}/{file}"
        s3_resource.Object(bucket, f"{destination_folder}/{file}").copy({'Bucket':bucket,'Key':my_key})
        ## After copying that file to old_data folder then delete that file from folder :-
        # s3_client.delete_object(Bucket=bucket, Key= source_folder + file_name)
        time.sleep(5)
        s3_resource.Object(bucket, f"{source_folder}/{file}").delete()
        new_folder='raw_data'
        s3_resource.Object(bucket, new_folder + '/').put()
        print(f'{file_name} is successfully moved to {destination_folder}.')
    except Exception as e:
        print(f"Error :failed to move file -{file_name} - {e}")
