from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
import json
import logging
import couchdb
import time

# Logging configs DB consumer service
logger = logging.getLogger('DB_consumer')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

"""
This class provides the DB consumer service.
Its connecting to an Kafka topic and to the couchDB,
ensures the topic exists and consumes messages from it.  
It will then, based on the event action interact with the database.
"""
class KafkaConsumerService:
    def __init__(self, broker_url: str, topic: str, group_id: str, db_url: str, db_name: str, db_username: str, db_password: str):
        self.broker_url = broker_url
        self.topic = topic
        self.group_id = group_id
        self.db_url = db_url
        self.db_name = db_name
        self.db_username = db_username
        self.db_password = db_password

        # consumer configuration
        self.consumer_config = {
            'bootstrap.servers': self.broker_url,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        }

        # Initialize consumer
        self.consumer = Consumer(self.consumer_config)
        
        # Kafka AdminClient to check if the topic exists
        self.admin_client = AdminClient({'bootstrap.servers': self.broker_url})

        # CouchDB setup
        self.couch = couchdb.Server(f'http://{self.db_username}:{self.db_password}@{self.db_url}')
        self.db = self.couch[self.db_name]
        logger.info(f"Connected to CouchDB server on host {self.db_url}")

        self.ensure_topic_exists()  # Ensure the topic exists before subscribing, else Kafka cries..

    # dont look at this..
    def ensure_topic_exists(self):
        while True:
            try:
                topics = self.admin_client.list_topics(timeout=10).topics
                if self.topic in topics:
                    logger.info(f"Topic '{self.topic}' exists!")
                    return
                else:
                    logger.info(f"Topic '{self.topic}' does not exist yet. Retrying...")
                    time.sleep(5)
            except Exception as e:
                logger.error(f"Failed to check topic existence: {e}")
                time.sleep(5)

    def consume_messages(self):
        try:
            self.consumer.subscribe([self.topic])
            logger.info(f"Connected to Kafka topic {self.topic} and ready to consume messages.")
            
            while True:
                # Not sure about this polling but whatever..
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    logger.info(f"No new messages in topic {self.topic}")
                    continue

                if msg.error():
                    raise KafkaException(msg.error())

                # Deserialization
                event = json.loads(msg.value().decode('utf-8'))

                # Do something based on the event action
                if event['action'] == 'create':
                    item_data = event['data']
                    doc_id, doc_rev = self.db.save(item_data)
                    logger.info(f"Item {item_data['name']} saved to CouchDB with id {doc_id}")

                if event['action'] == 'delete':
                    item_data = event['data']
                    try:
                        # Check if item exists
                        if item_data['id'] in self.db:
                            del self.db[item_data['id']]
                            logger.info(f"Item {item_data['id']} deleted from CouchDB.")
                        else:
                            logger.error(f"Item with id {item_data['id']} does not exist, cannot delete.")
                    except couchdb.http.ResourceNotFound:
                        # Item not in DB
                        logger.error(f"Failed to delete item with id {item_data['id']}: Item not found.")
                    except Exception as e:
                        # Everything else..
                        logger.error(f"Error deleting item {item_data['id']}: {e}")

        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")

        finally:
            self.consumer.close()
