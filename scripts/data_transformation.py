import pandas as pd
from datetime import datetime

def clean_weather_data(data):
    df = pd.DataFrame([data])
    df['dt'] = pd.to_datetime(df['dt'], unit='s')
    df = df.rename(columns={'main.temp': 'temperature', 'main.humidity': 'humidity', 'weather': 'description'})
    return df[['dt', 'temperature', 'humidity', 'description']]

def clean_traffic_data(data):
    routes = data.get('routes', [])
    if not routes:
        return pd.DataFrame()
    legs = routes[0].get('legs', [])
    if not legs:
        return pd.DataFrame()
    leg = legs[0]
    df = pd.DataFrame([leg])
    df['start_time'] = datetime.now()
    df = df.rename(columns={'distance.text': 'distance', 'duration.text': 'duration'})
    return df[['start_time', 'distance', 'duration']]

# Example usage
weather_df = clean_weather_data(weather_data)
traffic_df = clean_traffic_data(traffic_data)

print(weather_df)
print(traffic_df)
