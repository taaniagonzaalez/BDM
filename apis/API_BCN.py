import os
import requests
import boto3
from datetime import datetime
from dotenv import load_dotenv

def download_bcn_file():
    # URL of the file to download
    file_url = "https://opendata-ajuntament.barcelona.cat/data/dataset/b4d2cc2f-67dc-481a-a7cb-1999fd0d5740/resource/bce0486e-370e-4a72-903f-024ba8902ae1/download"

    # Send GET request and return the content
    response = requests.get(file_url)
    response.raise_for_status()  # raise exception if there's an error
    return response.content

def main():
    load_dotenv()
    s3 = boto3.client('s3')
    bucket_name = "bdm-project-upc"

    current_date = datetime.now().strftime('%Y%m%d')
    s3_key = f'raw/batch/barcelona/barcelona_raw_restaurants.csv'

    file_content = download_bcn_file()

    # Upload to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=file_content,
        ContentType='text/csv'
    )

    print(f"[S3] File uploaded to s3://{bucket_name}/{s3_key}")