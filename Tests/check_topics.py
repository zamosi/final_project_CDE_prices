import time
import logging
from configparser import ConfigParser
from kafka import KafkaAdminClient

# Load database configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Suppress Kafka's internal logs
logging.getLogger("kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def wait_for_topics(broker_url: str, topic_names: list, check_interval: int = 5):
    """
    Waits until all topics in the list appear in Kafka.
    """
    try:
        # Connect to Kafka Admin
        admin_client = KafkaAdminClient(bootstrap_servers=broker_url)
        
        logger.info(f"Waiting for topics {topic_names} to appear...")
        while True:
            try:
                # Fetch the list of existing topics
                topics = admin_client.list_topics()
                
                # Check if all topics exist
                missing_topics = [topic for topic in topic_names if topic not in topics]
                if not missing_topics:
                    logger.info(f"All topics {topic_names} have appeared.")
                    break
            except Exception as e:
                # Suppress detailed errors from appearing in logs
                pass
            
            # Wait before the next check
            time.sleep(check_interval)
    finally:
        admin_client.close()

# Example usage
if __name__ == "__main__":
    try:
        broker = config["Kafka"]["KAFKA_BOOTSTRAP_SERVERS"]
        if not broker:
            raise ValueError("Kafka broker URL not found in the configuration.")
        
        # Topics to monitor
        topics = ["snifim", "prices"]
        wait_for_topics(broker, topics)
    except Exception as e:
        logger.error(f"Error: {e}")
