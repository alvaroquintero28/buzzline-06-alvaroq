# Import necessary libraries
import os
import json
from collections import defaultdict
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

#####################################
# Getter Functions for Environment Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group ID from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group ID: {group_id}")
    return group_id

#####################################
# Initialize Data Structures
#####################################

# Initialize a dictionary to store counts
attribute_counts = defaultdict(int)
total_messages = 0

# Load the dataset for pre-processing if needed
dataset = pd.read_csv('/Users/alvaroquintero/Data Streaming/buzzline-04-alvaro/data/CARS_1.csv')

# Initialize counts using 'fuel_type' and 'transmission_type' from the dataset
for _, row in dataset.iterrows():
    key = (row['fuel_type'], row['transmission_type'])
    attribute_counts[key] += 1

#####################################
# Set up Plotting
#####################################

# Set up figure and axis for real-time plotting
fig, ax = plt.subplots()
plt.ion()  # Turn on interactive mode

def update_chart():
    """Update the live chart with the latest attribute averages."""
    ax.clear()

    # Calculate averages based on total messages processed
    if total_messages > 0:
        averages = {key: count / total_messages for key, count in attribute_counts.items()}
    else:
        averages = {}

    # Extract data points
    attributes_list = list(averages.keys())
    avg_values_list = list(averages.values())

    # Create bar chart for the averages
    ax.bar(range(len(attributes_list)), avg_values_list, color="skyblue")

    # Set labels and title
    ax.set_xlabel("Attributes (Fuel Type, Transmission Type)")
    ax.set_ylabel("Average Occurrences")
    ax.set_title("Real-Time Average Car Attributes by Alvaro Quintero")

    ax.set_xticks(range(len(attributes_list)))
    ax.set_xticklabels([f'{ft}, {tt}' for ft, tt in attributes_list], rotation=45, ha="right")

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

#####################################
# Message Processing
#####################################

def process_message(message: str) -> None:
    """Process a single JSON message from Kafka and update the chart."""
    global total_messages  # Access the global variable
    try:
        logger.debug(f"Raw message: {message}")
        message_dict: dict = json.loads(message)
        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            fuel_type = message_dict.get("fuel_type", "unknown")
            transmission_type = message_dict.get("transmission_type", "unknown")

            # Update counts and total messages
            attribute_counts[(fuel_type, transmission_type)] += 1
            total_messages += 1

            logger.info(f"Updated counts: {dict(attribute_counts)}")
            update_chart()
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main Function
#####################################

def main() -> None:
    """Main entry point for the Kafka consumer."""
    logger.info("START consumer.")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consuming from topic '{topic}' with group ID '{group_id}'...")
    consumer = create_kafka_consumer(topic, group_id)

    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")
        plt.ioff()
        plt.show()

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

if __name__ == "__main__":
    main()
