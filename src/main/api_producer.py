import json
import logging
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

# Setup logging for Producer service
logger = logging.getLogger('KafkaService')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

"""
This is the producer for the API.
Lets you send messages as dict to an Kafka topic.
"""
class KafkaService:
    def __init__(self, broker_url: str):
        if broker_url is None or broker_url == '':
            raise ValueError("Broker URL cannot be None or empty")
        
        # init the producer
        self.producer = Producer(
            {'bootstrap.servers': broker_url}
        )
        self.test_connection(broker_url)

    def test_connection(self, broker_url):
        try:
            admin_client = AdminClient({'bootstrap.servers': broker_url})
            admin_client.list_topics(timeout=10)
            logger.info(f"Kafka connection on {broker_url} successful!")
        except Exception as e:
            logger.error(f"Failed to establish a connection to Kafka: {e}")

    def send_message(self, topic: str, message: dict):
        try:
            serialized_message = json.dumps(message).encode('utf-8')
            self.producer.produce(topic, value=serialized_message, callback=self.delivery_report)
            self.producer.flush()
            logger.info(f"Message sent to Kafka topic '{topic}': {message}")
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")

    # this thing shows whats wrong when delivery unsuccessful...
    def delivery_report(self, err, msg):
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

