import models
from db_consumer import KafkaConsumerService

"""
Runner file for the DB consumer.
"""

app_settings = models.AppSettings()

if __name__ == '__main__':
    # Configurations for the consumer
    broker_url = app_settings.KAFKA_BROKER
    topic = app_settings.KAFKA_TOPIC
    group_id = app_settings.KAFKA_GROUP_ID
    db_url = f'{app_settings.DB_HOST}:{app_settings.DB_PORT}/'
    db_name = app_settings.DB_NAME
    db_username = app_settings.DB_USERNAME
    db_password = app_settings.DB_PASSWORD

    # print(f"DB Url: {db_url}")

    # Init
    kafka_consumer = KafkaConsumerService(
        broker_url=broker_url,
        topic=topic,
        group_id=group_id,
        db_url=db_url,
        db_name=db_name,
        db_username=db_username,
        db_password=db_password
    )
    print("Connected to Kafka topic")

    # Start consuming messages
    kafka_consumer.consume_messages()