# Microservice Demo
A demo for working with microservices.  
Contains a backend RestAPI written with FastAPI, a couchDB No-SQL Database and a Kafka Pub-Sub.  
The API will act as the producer and sent events to a Kafka topic, when an item is created, updated or deleted.  
The database consumer service will watch this topic and apply the according logic to the database.  

## Preqrequisites
- mise installed for easy setup (can also run everything one by one)
- A working Kubernetes cluster with an Apache Kafka (if you want to use Kafka, which kinda is the point of this demo)

## How to run locally
You need to have docker and python installed.  
This app can be run with or without Kafka.  

### Without Kafka

### With Kafka

Kafka commands:
```
# exec into the container
docker exec -it kafka bash
cd /opt/kafka/bin

# List topics
d55dc7950eb5:/opt/kafka/bin$ ./kafka-topics.sh --bootstrap-server kafka:9092 --list
items-topic

# List all items in topic
./kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic items-topic --from-beginning --partition 0
```

### Use

```
http://localhost:8080/docs
http://127.0.0.1:5984/_utils
```

## How to run on Kubernetes
