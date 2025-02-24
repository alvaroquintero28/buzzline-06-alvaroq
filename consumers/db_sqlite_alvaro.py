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
import matplotlib.dates as mdates

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
DATABASE_PATH = pathlib.Path(os.environ.get("DATABASE_PATH", "./data/sports_odds.sqlite"))
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sports_odds")

# Data structures
game_counts = defaultdict(int)
game_counts_over_time = []
game_scores = []

# Live visuals
plt.ion()
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 12))
ax4.axis('off') #remove redundant plot

def update_chart():
    ax1.clear()
    ax2.clear()
    ax3.clear()

    labels = list(game_counts.keys())
    counts = list(game_counts.values())

    if not labels or not counts:
        ax1.text(0.5, 0.5, "No data yet!", ha="center", va="center")
        ax2.text(0.5, 0.5, "No data yet!", ha="center", va="center")
        ax3.text(0.5, 0.5, "No data yet!", ha="center", va="center")
        plt.tight_layout()
        plt.draw()
        plt.pause(0.01)
        return

    # Bar chart
    ax1.bar(labels, counts, color="green")
    ax1.set_xlabel("Game ID")
    ax1.set_ylabel("Count")
    ax1.set_title("Real-Time Sports Game Counts (Bar Chart)")
    ax1.set_xticklabels(labels, rotation=45, ha="right")

    # Pie chart
    ax2.pie(counts, labels=labels, autopct='%1.1f%%', startangle=140)
    ax2.axis('equal')
    ax2.set_title("Real-Time Sports Game Counts (Pie Chart)")

    # Line Chart
    timestamps, counts_time = zip(*[(time.strftime('%H:%M:%S',time.localtime(timestamp)), count) for timestamp, game_id, count in game_counts_over_time])
    ax3.plot(timestamps, counts_time, marker='o', linestyle='-')
    ax3.set_xlabel("Time")
    ax3.set_ylabel("Game Count")
    ax3.set_title("Game Counts Over Time")
    plt.setp(ax3.get_xticklabels(), rotation=45, ha="right")

    # Histogram
    if game_scores:
        ax4.hist(game_scores, bins=10, color='skyblue', edgecolor='black')
        ax4.set_xlabel("Score")
        ax4.set_ylabel("Frequency")
        ax4.set_title("Distribution of Game Scores")

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
            consumer_timeout_ms=60000
        )
        logger.info(f"Kafka consumer connected to {KAFKA_BROKER}, consuming from {KAFKA_TOPIC}")

        for message in consumer:
            game_id = message.value.get("game_id", "Unknown")
            game_counts[game_id] += 1
            timestamp = time.time()
            game_counts_over_time.append((timestamp, game_id, game_counts[game_id]))
            score = message.value.get("score", 0)  # Assumes 'score' key exists in JSON
            game_scores.append(score)
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

