# Spotify-ETL-project
In this project, we are extracting Spotify-playlist data from spotify API and working on it. By the end of this project you will definitely learn the concept of ETL and datapipeline in detail.

The Basic Architechture of the project is shown below.

![Spotify-aws_project_architechture](https://github.com/Sarang823/Spotify-ETL-project/assets/133379507/b1d948eb-240d-4ad8-9437-821e3398e212)

## Technologies & Tools used :
1. Spotify Developer API
2. Python
3. AWS Lambda function
4. AWS S3 Bucket
5. AWS EventBridge
6. Python-Pandas

## Architechture working of Lambda Function:
![spotify_project_architecture_explanation](https://github.com/Sarang823/Spotify-ETL-project/assets/133379507/1e93b68c-8d63-49e5-abf6-857efe379dbf)


# Step By Step Guide:
1. Create Spotify Developer account. >> Create your first app >> And from there get **Client ID and Client secret ID**.
2. For AWS setup, create s3 bucket, and create 2 lambda functions(1 for extract and 1 for transform), create Eventbridge scheduler. Steps are explained in detail below. 
3. Step 2 is extracting the data from Spotify API and dumping that json data to s3Bucket, for this step refer to Extract_code.py
4. Step 3 is cleaning that raw json data and creating separate songs, albums and artist file out of it. This is done using Transform lambda function. For detail explanation refer the extract.py file
5. The last step includes creating a Glue crawler (assigning an IAM role with S3 Access) and querying the data using AWS Athena.


 
