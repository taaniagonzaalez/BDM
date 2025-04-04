import io
import pandas as pd
import os
import requests
import numpy as np
import random


folder_path = "dataframes"

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

    # Take interesting columns
    df_new = df[['name', 'created', 'modified', 'addresses_road_name','addresses_start_street_number', 'values_value', 'geo_epgs_4326_lat', 'geo_epgs_4326_lon']]
    df_new = df_new.rename(columns={'addresses_start_street_number':'street_number', 'values_value' : 'phone_number', 'geo_epgs_4326_lat': 'latitude', 'geo_epgs_4326_lon':'longitude'})
    df_new['Rating'] = [round(np.clip(np.random.normal(loc=4, scale=1), 0, 5), 1) for _ in range(len(df_new))]

    # Generate a random category for the restaurant (it may make no sense)
    categories = ["Tapas", "Japanese", "Italian", "Mexican", "Chinese", "Indian", "French", "American", "Vegan", "Thai", "Spanish"]
    df_new['Class'] = [random.choice(categories) for _ in range(len(df_new))]




