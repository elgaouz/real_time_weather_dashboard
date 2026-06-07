import time
import json
import os
from kafka import KafkaProducer
import requests

from time_utils import now_morocco_str


kafka_bootstrap_servers = "localhost:9094"
kafka_topic = "sample_topic"
# Lower = closer to “real time” (respect OpenWeather rate limits; default 1s).
WEATHER_POLL_SECONDS = float(os.getenv("WEATHER_POLL_SECONDS", "1"))

producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def get_weather_detail(openweathermap_api_endpoint: str) -> dict:
    try:
        api_response = requests.get(openweathermap_api_endpoint, timeout=10)
        if api_response.status_code != 200:
            print("OpenWeather error:", api_response.status_code, api_response.text)
            api_response.raise_for_status()
        json_data = api_response.json()

        creation_time = now_morocco_str()

        return {
            "CityName": json_data["name"],
            "Temperature": json_data["main"]["temp"],
            "Humidity": json_data["main"]["humidity"],
            "CreationTime": creation_time,
        }
    except Exception as e:
        # Fallback: if API key is invalid or request fails, send dummy data
        print("OpenWeather request failed, sending dummy data instead:", e)
        creation_time = now_morocco_str()
        return {
            "CityName": "Rabat",
            "Temperature": 25.0,
            "Humidity": 50,
            "CreationTime": creation_time,
        }


def get_appid() -> str:
    appid = os.getenv("OPENWEATHER_API_KEY")
    if not appid:
        raise ValueError(
            "OPENWEATHER_API_KEY not found in environment variables. "
            "Set it before running the Kafka producer."
        )
    return appid


if __name__ == "__main__":
    appid = get_appid()
    cities = ["Rabat", "Casablanca", "Marrakech"]
    counter = 0

    while True:
        for city in cities:
            endpoint = (
                "http://api.openweathermap.org/data/2.5/weather"
                f"?q={city}&appid={appid}&units=metric"
            )
            message = get_weather_detail(endpoint)
            producer.send(kafka_topic, value=message)
            producer.flush()
            counter += 1
            print(f"Sent message {counter} for {city}: {message}")
            time.sleep(WEATHER_POLL_SECONDS)

