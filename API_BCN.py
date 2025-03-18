import io
import pandas as pd
import os
import requests

folder_path = "dataframes"

"""
DATA FROM BARCELONA API (restaurant names, addresses, phones, etc)
"""

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

df_new = df[['name', 'created', 'modified', 'addresses_road_name','addresses_start_street_number', 'values_value', 'geo_epgs_4326_lat', 'geo_epgs_4326_lon']]
df_new = df_new.rename(columns={'addresses_start_street_number':'street_number', 'values_value' : 'phone_number', 'geo_epgs_4326_lat': 'latitude', 'geo_epgs_4326_lon':'longitude'})

df_new.to_csv(f'{folder_path}/restaurants_bcn.csv', index=False)