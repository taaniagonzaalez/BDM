import numpy as np
import random
from datetime import datetime
import boto3
import os
import pandas as pd
import json
from dotenv import load_dotenv
import time


"""

GET FAKE REVIEWS FROM GOOGLE MAPS

"""

class API_GM():
    def __init__(self, restaurant):
        self.restaurant = restaurant

    def download_kaggle_csv_to_dataframe(self):
        path = '/opt/airflow/apis/Restaurant_reviews.csv'
        df = pd.read_csv(path, sep=',')
        return df
    
    def generate_review(self):
        # Generate a review

        df = self.download_kaggle_csv_to_dataframe()
        reviews = df[['Reviewer', 'Review', 'Rating']].to_dict(orient="records")
        return {
            "review_object": random.choice(reviews),
            "timestamps": datetime.now().isoformat()
        }
    
    def generate_multiple_reviews(self, num=10):
        # Generate multiple reviews
        reviews = {'restaurant': self.restaurant, 'reviews' : []}
        for _ in range(num):
              review = self.generate_review()
              reviews['reviews'].append(review)
        return reviews
    
    def get_current_status(self):
        # Get a closed or opened output
        rn = datetime.now().hour
        if 11 <= rn <= 17 or 20 <= rn <= 00:
             status = 'Opened'
        else:
             status = 'Closed'
        return status
    
    def get_number_people(self):
        # Get the number of people that are in the restaurant. With Kafka, we are going to generate a sentence saying if 
        #there are a lot of people or not.
        number_people = random.randint(1,100)
        return number_people

"""
GET RESTAURANT INFO
"""
def get_restaurant_review(restaurant):
    api = API_GM(restaurant)
    return api.generate_multiple_reviews()

def main():
    load_dotenv()
    s3 = boto3.client('s3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION")
        )
    bucket_name = "bdm-project-upc"
    s3_path_bcn = 's3://bdm-project-upc/raw/batch/barcelona/barcelona_raw_restaurants.csv'
    df = pd.read_csv(
            s3_path_bcn,
            sep='\t',                # separador correcto
            encoding='utf-16'     # reconoce y elimina el BOM
        )
    date = datetime.now().isoformat().split("T")[0]
    for row in df.head(3).itertuples():
        review = get_restaurant_review(row.name)
        s3_key = f"raw/batch/googlemaps/date={date}/restaurant_{row.name.strip()}.csv"
        s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(review),
        ContentType='application/json'
        )
        print(f'[S3]Restaurant {row.name} uploaded correctly')

if __name__ == "__main__":
    main()





