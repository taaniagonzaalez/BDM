import requests
import json
import os
import boto3

# Configuración de API Foursquare
FOURSQUARE_API_KEY = "fsq33IomRNg9y+33yeWoBDYszC3kkKYDEysBR3/Wyf8kJC0="
FOURSQUARE_BASE_URL = "https://api.foursquare.com/v3/places/search"
FOURSQUARE_PHOTOS_URL = "https://api.foursquare.com/v3/places/{fsq_id}/photos"

def obtener_restaurantes_foursquare(ciudad):
    headers = {
        "Accept": "application/json",
        "Authorization": FOURSQUARE_API_KEY
    }
    params = {"query": "restaurant", "near": ciudad, "limit": 5}
    try:
        respuesta = requests.get(FOURSQUARE_BASE_URL, headers=headers, params=params)
        respuesta.raise_for_status()
        return respuesta.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error en la API de Foursquare: {e}")
        return []

def obtener_fotos_restaurante_foursquare(fsq_id, cantidad=1):
    headers = {
        "Accept": "application/json",
        "Authorization": FOURSQUARE_API_KEY
    }
    url = FOURSQUARE_PHOTOS_URL.format(fsq_id=fsq_id)
    params = {"limit": cantidad}

    try:
        respuesta = requests.get(url, headers=headers, params=params)
        respuesta.raise_for_status()
        fotos = respuesta.json()
        if fotos:
            return [
                f"{foto['prefix']}original{foto['suffix']}"
                for foto in fotos
            ]
        return []
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener fotos para {fsq_id}: {e}")
        return []

# Lógica principal
ciudad = "Barcelona"
nombre_archivo_s3 = "restaurantes_barcelona.json"

print("Consultando Foursquare...")
datos = []
datos_fsq = obtener_restaurantes_foursquare(ciudad)

for r in datos_fsq:
    fsq_id = r.get("fsq_id")
    r["photo_urls"] = obtener_fotos_restaurante_foursquare(fsq_id)
    datos.append(r)

if datos:
    for r in datos:
        print("\n====================")
        print(f"Nombre: {r.get('name')}")
        print(f"Dirección: {r.get('location', {}).get('formatted_address')}")
        print(f"Fotos Foursquare: {r.get('photo_urls')}")

    # Guardar en S3
    s3 = boto3.client("s3")
    bucket_name = os.getenv("AWS_BUCKET_NAME", "bdm-project-upc")
    contenido = json.dumps(datos, indent=2)
    ruta_s3 = f"restaurantes/{nombre_archivo_s3}"
    try:
        s3.put_object(Body=contenido, Bucket=bucket_name, Key=ruta_s3)
        print(f"Datos guardados en S3: s3://{bucket_name}/{ruta_s3}")
    except Exception as e:
        print(f"Error al guardar en S3: {e}")