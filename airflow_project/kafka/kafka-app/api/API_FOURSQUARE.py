import requests

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