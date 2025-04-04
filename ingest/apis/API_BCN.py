import io
import pandas as pd
import requests
import numpy as np
import random
import sys
import os
from datetime import datetime
import boto3
import json


"""
DATA FROM BARCELONA API (restaurant names, addresses, phones, etc)
"""
def bcn_data():
    # URL of the file to download
    file_url = "https://opendata-ajuntament.barcelona.cat/data/dataset/b4d2cc2f-67dc-481a-a7cb-1999fd0d5740/resource/bce0486e-370e-4a72-903f-024ba8902ae1/download"

    # Send GET request
    response = requests.get(file_url)

    # Store file content in a variable
    file_text = response.text  

    # Remove BOM (Byte Order Mark) and decode properly
    decoded_data = file_text.encode("latin1").decode("utf-16")

    # Convert to DataFrame using StringIO and modify it
    df = pd.read_csv(io.StringIO(decoded_data), delimiter="\t")

    # Generate a random category for the restaurant (it may make no sense)
    # categories = ["Tapas", "Japanese", "Italian", "Mexican", "Chinese", "Indian", "French", "American", "Vegan", "Thai", "Spanish"]
    # df_new['Class'] = [random.choice(categories) for _ in range(len(df_new))]
    
    return df.to_dict(orient="records")

if __name__ == "__main__":

    s3 = boto3.client('s3')
    bucket_name = os.getenv("AWS_BUCKET_NAME", "bdm-project-upc")

    current_date = datetime.now().strftime('%Y-%m-%d')
    s3_path = f's3a://{bucket_name}/raw/batch/barcelona/date={current_date}/'
    
    s3.put_object(
            Bucket=bucket_name,
            Key=s3_path,
            Body=json.dumps(bcn_data()),
            ContentType='application/json'
        )

    print(f"[S3] Saved in {s3_path}")







