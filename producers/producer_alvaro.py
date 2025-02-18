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
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")  # Your Kafka broker address
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sports_odds")  # Your Kafka topic name
ODDS_API_KEY = os.environ.get("ODDS_API_KEY")  # Your Odds API key (KEEP SECURE!)
MESSAGE_INTERVAL_SECONDS = int(os.environ.get("MESSAGE_INTERVAL_SECONDS", "5")) #Sets the interval in which the data is sent
LIVE_DATA_PATH = pathlib.Path(os.environ.get("LIVE_DATA_PATH", "./data/live_data.json")) #Sets the path for the file


# --- Helper Functions ---
def create_kafka_producer():
    """Creates and returns a Kafka producer. Handles exceptions gracefully."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3, # Retry up to 3 times on connection errors
            retry_backoff_ms=1000 # Wait 1 second between retries
        )
        logger.info(f"Kafka producer connected to {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to create Kafka producer: {e}. Exiting.")
        exit(1)


def get_odds_data(sport, region):
    """Fetches odds data from The Odds API. Includes detailed error handling."""
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds/?apiKey={ODDS_API_KEY}&regions={region}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from The Odds API: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON response from The Odds API: {e}")
        return None


def send_to_kafka(producer, message):
    """Sends a message to Kafka with error handling."""
    try:
        producer.send(KAFKA_TOPIC, value=message)
        logger.info(f"Sent message to Kafka: {message}")
    except Exception as e:
        logger.error(f"Error sending message to Kafka: {e}")


def main():
    """Main function to orchestrate data fetching and Kafka sending."""

    # Create a Kafka Producer
    producer = create_kafka_producer()
    if not producer:
        return  # Exit if producer creation failed

    #Ensure file is deleted before starting the script.
    if LIVE_DATA_PATH.exists():
        LIVE_DATA_PATH.unlink()
        logger.info("Deleted existing live data file.")

    os.makedirs(LIVE_DATA_PATH.parent, exist_ok=True)


    sport = "basketball_nba"  # Example; change to your desired sport
    region = "us"  # Example; change to your desired region

    while True:
        odds_data = get_odds_data(sport, region)
        if odds_data:
            for game in odds_data:
                #Prepare the message to be sent to Kafka.  Adapt to your needs.
                message = {
                    "sport": sport,
                    "region": region,
                    "game_id": game.get("id", "N/A"), # Use .get() to handle missing keys safely
                    "teams": game.get("teams", []), # Use .get() to handle missing keys safely
                    "sites": game.get("sites", []),  # Use .get() to handle missing keys safely
                  #Add other relevant fields here
                }
                send_to_kafka(producer, message)

                with LIVE_DATA_PATH.open("a") as f:
                    f.write(json.dumps(message) + "\n")
                    logger.info(f"Wrote message to file: {message}")

        time.sleep(MESSAGE_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
