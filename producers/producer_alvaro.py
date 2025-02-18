import os
import json
import time
import logging
from kafka import KafkaProducer
import requests
import pathlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration (Consider using environment variables for security) ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sports_odds")
ODDS_API_KEY = os.environ.get("ODDS_API_KEY")
MESSAGE_INTERVAL_SECONDS = int(os.environ.get("MESSAGE_INTERVAL_SECONDS", "5"))
LIVE_DATA_PATH = pathlib.Path(os.environ.get("LIVE_DATA_PATH", "./data/live_data.json"))

# --- Helper Functions ---
def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            retry_backoff_ms=1000
        )
        logger.info(f"Kafka producer connected to {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to create Kafka producer: {e}. Exiting.")
        exit(1)


def get_odds_data(sport, region):
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds/?apiKey={ODDS_API_KEY}&regions={region}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from The Odds API: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON response from The Odds API: {e}")
        return None


def send_to_kafka(producer, message):
    try:
        producer.send(KAFKA_TOPIC, value=message)
        logger.info(f"Sent message to Kafka: {message}")
    except Exception as e:
        logger.error(f"Error sending message to Kafka: {e}")


def main():
    producer = create_kafka_producer()
    if not producer:
        return

    if LIVE_DATA_PATH.exists():
        LIVE_DATA_PATH.unlink()
        logger.info("Deleted existing live data file.")
    os.makedirs(LIVE_DATA_PATH.parent, exist_ok=True)

    sport = "basketball_nba"
    region = "us"

    while True:
        odds_data = get_odds_data(sport, region)
        if odds_data:
            for game in odds_data:
                message = {
                    "sport": sport,
                    "region": region,
                    "game_id": game.get("id", "N/A"),
                    "sites": game.get("sites", [])
                }
                send_to_kafka(producer, message)
                with LIVE_DATA_PATH.open("a") as f:
                    f.write(json.dumps(message) + "\n")
                    logger.info(f"Wrote message to file: {message}")
        time.sleep(MESSAGE_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
