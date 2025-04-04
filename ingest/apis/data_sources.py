import requests

FOURSQUARE_API_KEY = "fsq33IomRNg9y+33yeWoBDYszC3kkKYDEysBR3/Wyf8kJC0="
FOURSQUARE_BASE_URL = "https://api.foursquare.com/v3/places/search"
FOURSQUARE_PHOTOS_URL = "https://api.foursquare.com/v3/places/{fsq_id}/photos"

GOOGLE_API_KEY = "YOUR_GOOGLE_API_KEY"
GOOGLE_PLACE_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
GOOGLE_PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

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

def obtener_info_google(nombre, direccion):
    try:
        search_params = {
            "input": f"{nombre} {direccion}",
            "inputtype": "textquery",
            "fields": "place_id",
            "key": GOOGLE_API_KEY
        }
        search_resp = requests.get(GOOGLE_PLACE_SEARCH_URL, params=search_params).json()
        candidates = search_resp.get("candidates")
        if not candidates:
            return {}

        place_id = candidates[0]["place_id"]

        detail_params = {
            "place_id": place_id,
            "fields": "rating,user_ratings_total,opening_hours,reviews,photo",
            "key": GOOGLE_API_KEY
        }
        detail_resp = requests.get(GOOGLE_PLACE_DETAILS_URL, params=detail_params).json()
        result = detail_resp.get("result", {})

        photo_urls = []
        if "photos" in result:
            for photo in result["photos"][:1]:
                ref = photo["photo_reference"]
                photo_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photoreference={ref}&key={GOOGLE_API_KEY}"
                photo_urls.append(photo_url)

        return {
            "google_rating": result.get("rating"),
            "google_reviews": result.get("user_ratings_total"),
            "google_opening_hours": ", ".join(result.get("opening_hours", {}).get("weekday_text", [])),
            "google_comments": [r.get("text") for r in result.get("reviews", [])[:3]],
            "google_photo_urls": photo_urls
        }
    except Exception as e:
        print(f"Error al obtener datos de Google Maps: {e}")
        return {}