import os
import json
import sqlite3
import logging
from kafka import KafkaConsumer
import pathlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Configuration (Use environment variables for security and flexibility) ---
DATABASE_PATH = pathlib.Path(os.environ.get("DATABASE_PATH", "./data/sports_odds.sqlite")) #Sets the path to the database
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092") #Sets the address for the Kafka broker
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sports_odds")  #Sets the topic to read from


# --- Helper functions ---

def create_database_table(db_path):
    """Initializes the SQLite database and creates the messages table if needed."""
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sports_odds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT
            )
        """)
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {db_path} and table created.")
    except sqlite3.Error as e:
        logger.error(f"Error creating or initializing database at {db_path}: {e}")


def insert_message(db_path, message):
    """Inserts a single message into the database with error handling."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO sports_odds (message) VALUES (?)", (json.dumps(message),))
        conn.commit()
        conn.close()
        logger.info(f"Message inserted into database: {message}")
    except sqlite3.Error as e:
        logger.error(f"Error inserting message into database: {e}")



def consume_and_store():
    """Consumes messages from Kafka and stores them in the SQLite database."""
    create_database_table(DATABASE_PATH) # create database table if it doesn't exist

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest', # Start consuming from the latest messages
        )
        logger.info(f"Kafka consumer connected to {KAFKA_BROKER}, consuming from {KAFKA_TOPIC}")

        for message in consumer:
            insert_message(DATABASE_PATH, message.value)

    except Exception as e:
        logger.error(f"Error consuming messages from Kafka: {e}")
    finally:
        if 'consumer' in locals() and consumer is not None:
            consumer.close()
            logger.info("Kafka Consumer closed.")


def main():
    """Main function to test."""
    consume_and_store()

if __name__ == "__main__":
    main()

