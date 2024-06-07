import requests
import os


class Extract:

    def __init__(self):
        # OpenWeatherMap API
        self.weather_api_key = os.getenv('WEATHER_API_KEY')
        if not self.weather_api_key:
            raise EnvironmentError("WEATHER_API_KEY environment variable is not set. Unable to call OpenWeatherAPI.")
        self.weather_url = 'http://api.openweathermap.org/data/2.5/weather'

        # OSRM (Traffic) API
        self.maps_url = 'http://router.project-osrm.org/route/v1/driving/'

    def get_weather(self, latitude, longitude, exclude=None, units='metric', lang=None):
        params = {
            'lon': longitude,
            'lat': latitude,
            'appid': self.weather_api_key,
            'units': units
        }
        if exclude:
            params['exclude'] = exclude
        if lang:
            params['lang'] = lang

        response = requests.get(self.weather_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error Occurred calling weather API.\n"
                            f"Error: {response.status_code}, 'message': {response.reason}")

    def get_traffic(self, start_coords, end_coords):
        coordinates = f"{start_coords[0]},{start_coords[1]};{end_coords[0]},{end_coords[1]}"
        url = f"{self.maps_url}{coordinates}"
        response = requests.get(url)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error Occurred calling traffic API.\n"
                            f"Error: {response.status_code}, 'message': {response.reason}")
