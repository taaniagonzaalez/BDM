import requests

FOURSQUARE_API_KEY = "fsq33IomRNg9y+33yeWoBDYszC3kkKYDEysBR3/Wyf8kJC0="
FOURSQUARE_BASE_URL = "https://api.foursquare.com/v3/places/search"
FOURSQUARE_PHOTO_URL = "https://api.foursquare.com/v3/places/{}/photos"

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
        restaurantes = respuesta.json().get("results", [])

        # Agregar fotos a cada restaurante
        for restaurante in restaurantes:
            place_id = restaurante.get("fsq_id")
            if place_id:
                fotos = obtener_fotos_restaurante(place_id)
                restaurante["photos"] = fotos

        return restaurantes
    except requests.exceptions.RequestException as e:
        print(f"Error en la API de Foursquare: {e}")
        return []

def obtener_fotos_restaurante(place_id):
    """Obtiene las fotos de un restaurante dado su ID en Foursquare."""
    headers = {
        "Accept": "application/json",
        "Authorization": FOURSQUARE_API_KEY
    }
    url = FOURSQUARE_PHOTO_URL.format(place_id)

    try:
        respuesta = requests.get(url, headers=headers)
        respuesta.raise_for_status()
        fotos = respuesta.json()

        # Construir las URLs de las fotos (Foursquare devuelve el prefijo y sufijo separados)
        foto_urls = [
            f"{foto['prefix']}original{foto['suffix']}" for foto in fotos
        ]
        return foto_urls[:3]  # Devolver máximo 3 fotos por restaurante
    except requests.exceptions.RequestException as e:
        print(f"Error obteniendo fotos para {place_id}: {e}")
        return []
