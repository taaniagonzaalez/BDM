import numpy as np
import random
from datetime import datetime
import json
import os
import zipfile
import pandas as pd
import time

"""

GET FAKE REVIEWS FROM GOOGLE MAPS

"""

class API_GM():
    def __init__(self, restaurant):
        self.restaurant = restaurant

    def download_and_extract_kaggle_dataset(self, dataset_api_name, output_folder, kaggle_username="taniiagoonzalez" , kaggle_key="2b50faae1acf0eabe02870abbaab798e"):
        """
        Downloads and extracts a dataset from Kaggle using the Kaggle API.
        
        Args:
            dataset_api_name (str): Format 'username/dataset-name'
            output_folder (str): Folder to extract contents into
            kaggle_username (str): Your Kaggle username
            kaggle_key (str): Your Kaggle API key
        """
        os.environ['KAGGLE_USERNAME'] = kaggle_username
        os.environ['KAGGLE_KEY'] = kaggle_key

        zip_name = dataset_api_name.split("/")[-1] + ".zip"

        # Download if not already downloaded
        if not os.path.exists(zip_name):
            print(f"Downloading {dataset_api_name}...")
            os.system(f"kaggle datasets download -d {dataset_api_name}")
        else:
            print(f"{zip_name} already downloaded.")

        # Extract if not already extracted
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            with zipfile.ZipFile(zip_name, 'r') as zip_ref:
                zip_ref.extractall(output_folder)
            print(f"Extracted to {output_folder}")
        else:
            print(f"{output_folder} already exists.")

        return output_folder
    
    def generate_review(self):
        # Generate a review

        path_names = self.download_and_extract_kaggle_dataset(
            "joebeachcapital/1000-restaurant-reviews",
            "api/google_maps_api/review"
            )
        
        df_review = pd.DataFrame(path_names)[['Reviewer', 'Review', 'Rating']].to_dict(orient="records")
        

        return {"restaurant_name": self.restaurant,
            "review":random.choice(df_review),
            "timestamps": datetime.now()}
    
    def generate_multiple_reviews(self, num=10):
        # Generate multiple reviews
        reviews = {'reviews' : []}
        for _ in range(num):
              ranking = self.generate_ranking()
              review = self.generate_review(ranking)
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
def get_restaurant_info(restaurant):
    api = API_GM(restaurant)
    return {
        "restaurant": restaurant,
        "status": api.get_current_status(),
        "people": api.get_number_people()
    }

def get_restaurant_review(restaurant):
    api = API_GM(restaurant)
    return api.generate_review()


"""
Send GM reviews
"""

def send_reviews(producer, restaurants, kafka, topic):
    while True:
        restaurante = random.choice(restaurants)
        review_data = get_restaurant_review(restaurante)
        mensaje = json.dumps(review_data).encode("utf-8")
        kafka.enviar_a_kafka(producer, topic, [mensaje])
        time.sleep(5)  # wait 5 seconds