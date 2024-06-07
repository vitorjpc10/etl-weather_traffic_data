from datetime import datetime, timezone
from polyline import decode


class Transform:
    def clean_weather_data(self, data):
        # Check if the necessary keys are present
        required_keys = ['coord', 'weather', 'main', 'dt', 'sys', 'name']
        for key in required_keys:
            if key not in data:
                raise KeyError(f"Missing required key: '{key}' in weather data.")

        # Extracting coordinates
        coordinates = data['coord']
        longitude = coordinates.get('lon', None)
        latitude = coordinates.get('lat', None)

        # Extracting weather details
        weather = data['weather'][0] if data['weather'] else None
        description = weather.get('description', None) if weather else None

        # Extracting main weather parameters
        main_weather = data['main']
        temperature = main_weather.get('temp', None)
        feels_like = main_weather.get('feels_like', None)
        temp_min = main_weather.get('temp_min', None)
        temp_max = main_weather.get('temp_max', None)
        pressure = main_weather.get('pressure', None)
        humidity = main_weather.get('humidity', None)
        visibility = data.get('visibility', None)

        # Extracting other relevant information
        timestamp = data.get('dt', None)
        sunrise = data['sys'].get('sunrise', None)
        sunset = data['sys'].get('sunset', None)
        name = data.get('name', None)
        country = data['sys'].get('country', None)
        wind_speed = data['wind'].get('speed', None)
        wind_degree_direction = data['wind'].get('deg', None)

        # Constructing the cleaned weather data dictionary
        weather_info = {
            'name': name,
            'country': country,
            'longitude': longitude,
            'latitude': latitude,
            'description': description,
            'temperature': temperature,
            'feels_like': feels_like,
            'temp_min': temp_min,
            'temp_max': temp_max,
            'pressure': pressure,
            'humidity': humidity,
            'visibility': visibility,
            'wind_speed': wind_speed,
            'wind_degree_direction': wind_degree_direction,
            'timestamp': datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S') if timestamp else None,
            'sunrise': datetime.utcfromtimestamp(sunrise).strftime('%Y-%m-%dT%H:%M:%S') if sunrise else None,
            'sunset': datetime.utcfromtimestamp(sunset).strftime('%Y-%m-%dT%H:%M:%S') if sunset else None
        }

        return weather_info

    def clean_traffic_data(self, data):
        if 'routes' not in data or not data['routes']:
            raise KeyError("Missing or empty 'routes' in traffic data.")

        route = data['routes'][0]

        if 'waypoints' not in data or len(data['waypoints']) < 2:
            raise KeyError("Missing or insufficient 'waypoints' in traffic data. "
                           "Expecting at least 2 for origin and destination waypoint")

        waypoints = data['waypoints']

        origin = waypoints[0]['location']
        destination = waypoints[1]['location']
        origin_name = waypoints[0].get('name', None)
        destination_name = waypoints[1].get('name', None)

        distance = route.get('distance', None)
        duration = route.get('duration', None)
        geometry = route.get('geometry', None)

        formatted_traffic_data = {
            'start_time': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
            'origin_latitude': origin[1],
            'origin_longitude': origin[0],
            'destination_latitude': destination[1],
            'destination_longitude': destination[0],
            'origin_name': origin_name,
            'destination_name': destination_name,
            'distance': distance,
            'duration': duration,
            'geometry': geometry,
            'route_coordinates': str(self.get_traffic_route_cordinates(data))
        }

        return formatted_traffic_data

    def get_traffic_route_cordinates(self, data):
        if data['code'] != 'Ok':
            raise ValueError("Error in response from OSRM API")

        route = data['routes'][0]
        geometry = route['geometry']
        decoded_geometry = decode(geometry)  # Decode polyline to get coordinates

        return decoded_geometry


# transform = Transform()
#
# # Example usage
# weather_df = transform.clean_weather_data(weather_data)
# traffic_df = transform.clean_traffic_data(traffic_data)
#
# print(weather_df)
# print(traffic_df)
