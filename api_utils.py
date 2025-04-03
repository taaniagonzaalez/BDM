import requests

FOURSQUARE_API_KEY = "fsq33IomRNg9y+33yeWoBDYszC3kkKYDEysBR3/Wyf8kJC0="
FOURSQUARE_BASE_URL = "https://api.foursquare.com/v3/places/search"

def obtener_restaurantes(ciudad, limite=5):
    """Obtiene restaurantes de Foursquare."""
    headers = {
        "Accept": "application/json",
        "Authorization": FOURSQUARE_API_KEY
    }
    params = {"query": "restaurant", "near": ciudad, "limit": limite}
    try:
        respuesta = requests.get(FOURSQUARE_BASE_URL, headers=headers, params=params)
        respuesta.raise_for_status()
        return respuesta.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error en la API de Foursquare: {e}")
        return []
