import requests

# OpenWeatherMap API
weather_api_key = 'your_openweather_api_key'
weather_url = 'http://api.openweathermap.org/data/2.5/weather'

# Google Maps API
maps_api_key = 'your_google_maps_api_key'
maps_url = 'https://maps.googleapis.com/maps/api/directions/json'

def get_weather(city):
    params = {'q': city, 'appid': weather_api_key, 'units': 'metric'}
    response = requests.get(weather_url, params=params)
    return response.json()

def get_traffic(origin, destination):
    params = {'origin': origin, 'destination': destination, 'key': maps_api_key}
    response = requests.get(maps_url, params=params)
    return response.json()

# Example usage
weather_data = get_weather('New York')
traffic_data = get_traffic('New York, NY', 'Los Angeles, CA')

print(weather_data)
print(traffic_data)
