import os
import json
import sqlite3
import logging
from collections import defaultdict 
from kafka import KafkaConsumer
import pathlib
import time
# Import external packages
from dotenv import load_dotenv

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt
# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################
def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Configuration (Use environment variables for security and flexibility) ---
DATABASE_PATH = pathlib.Path(os.environ.get("DATABASE_PATH", "./data/sports_odds.sqlite")) #Sets the path to the database
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092") #Sets the address for the Kafka broker
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sports_odds")  #Sets the topic to read from

#####################################
# Set up data structures
#####################################

# Initialize a dictionary to store author counts
author_counts = defaultdict(int)

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

def update_chart():
    """Update the live chart with the latest author counts."""
    # Clear the previous chart
    ax.clear()

    # Get the authors and counts from the dictionary
    authors_list = list(author_counts.keys())
    counts_list = list(author_counts.values())

    # Create a bar chart using the bar() method.
    # Pass in the x list, the y list, and the color
    ax.bar(authors_list, counts_list, color="green")

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Team")
    ax.set_ylabel("Counts")
    ax.set_title("Real Time Sports Odds by Alvaro Quintero") # Change the name to Alvaro Quintero

    # Use the set_xticklabels() method to rotate the x-axis labels
    # Pass in the x list, specify the rotation angle is 45 degrees,
    # and align them to the right
    # ha stands for horizontal alignment
    ax.set_xticklabels(authors_list, rotation=45, ha="right")

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)
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