import os
import json
import sqlite3
import logging
from collections import defaultdict
from kafka import KafkaConsumer
import pathlib
import time
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
DATABASE_PATH = pathlib.Path(os.environ.get("DATABASE_PATH", "./data/sports_odds.sqlite"))
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sports_odds")

# Data structures
game_counts = defaultdict(int) # Count games instead of teams

# Live visuals
plt.ion()
fig, ax = plt.subplots()


def update_chart():
    ax.clear()
    labels = list(game_counts.keys())
    counts = list(game_counts.values())
    ax.bar(labels, counts, color="green")
    ax.set_xlabel("Game ID")  # Updated x-axis label
    ax.set_ylabel("Count")
    ax.set_title("Real-Time Sports Game Counts")  # Updated title
    ax.set_xticklabels(labels, rotation=45, ha="right")
    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)


def create_database_table(db_path):
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
        logger.error(f"Error creating or initializing database: {e}")


def insert_message(db_path, message):
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
    create_database_table(DATABASE_PATH)
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=30000  #Increased timeout
        )
        logger.info(f"Kafka consumer connected to {KAFKA_BROKER}, consuming from {KAFKA_TOPIC}")

        for message in consumer:
            game_id = message.value.get("game_id", "Unknown")
            game_counts[game_id] += 1
            insert_message(DATABASE_PATH, message.value)
            update_chart()

    except Exception as e:
        logger.error(f"Error consuming messages from Kafka: {e}")
    finally:
        if 'consumer' in locals() and consumer is not None:
            consumer.close()
            logger.info("Kafka Consumer closed.")


def main():
    consume_and_store()
    plt.show(block=True)

if __name__ == "__main__":
    main()

