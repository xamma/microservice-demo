from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import models
import logging
import couchdb
import json
import redis
from api_producer import KafkaService

#-Logging configuration----------------------------
logger = logging.getLogger('API_Logger')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# FastAPI app
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# App settings
app_settings = models.AppSettings()

# CouchDB setup
try:
    couch = couchdb.Server(f'http://{app_settings.DB_USERNAME}:{app_settings.DB_PASSWORD}@{app_settings.DB_HOST}:{app_settings.DB_PORT}/')
    logger.info(f"Connected to Database on Host {app_settings.DB_HOST}")
    db_name = app_settings.DB_NAME

except:
    logger.error("Connection to Database failed")

# Create the database if not existant
if db_name not in couch:
    couch.create(db_name)

db = couch[db_name]

# Kafka setup
if app_settings.KAFKA_ENABLED == "true":
    logger.info("Kafka integration enabled.")
    kafka_service = KafkaService(broker_url=app_settings.KAFKA_BROKER)
else:
    logger.info("Kafka integration disabled.")

# Redis setup
try:
    redis_client = redis.Redis(
        host=app_settings.REDIS_HOST,
        port=app_settings.REDIS_PORT,
        db=app_settings.REDIS_DB,
        decode_responses=True
    )
    logger.info("Redis connection successful.")
except:
    logger.error("Connection to Redis failed.")


# POST - Create an item
@app.post("/items/", response_model=dict, status_code=201)
async def create_item(item: models.Item):
    logger.info("POST item route called")
    doc = item.model_dump(exclude_unset=True) # this is the old dict() to get the key/vals from the model

    # Send Kafka event for item creation
    if app_settings.KAFKA_ENABLED == "true":
        event_message = {
            'action': 'create',
            'data': {
                'name': item.name,
                'description': item.description,
                'price': item.price,
                'tax': item.tax
            }
        }
        kafka_service.send_message(app_settings.KAFKA_TOPIC, event_message)

        logger.info(f"New item {item.name} with id {item.id} sent to Kafka topic {app_settings.KAFKA_TOPIC}")

        resObj = {
            "message": f"Create request sent to Kafka topic {app_settings.KAFKA_TOPIC}",
            "status_code": 200
        }
        return resObj
    
    # this is happening when Kafka integration is disabled
    else:
        doc_id, doc_rev = db.save(doc) # returns an documentID and revision, we update the Item with the documentID
        item.id = doc_id  # Add the CouchDB document ID
        logger.info(f"New item {item.name} created with id {item.id}")
        return item.model_dump()

# GET - Get an Item by ID
# The read is directly on the DB, with or without Kafka integration
@app.get("/items/{item_id}", response_model=models.Item, status_code=200)
async def get_item(item_id: str):
    logger.info("GET item route called")
    # Check Redis cache first
    cached_item = redis_client.get(f"item:{item_id}")
    if cached_item:
        # If item is found in cache, return it directly
        logger.info(f"Cache hit for item {item_id}")
        
        try:
            # Deserialize the cached JSON string into a Python dictionary
            cached_item_dict = json.loads(cached_item)
            # Validate and return the item using Pydantic's model_validate
            return models.Item.model_validate(cached_item_dict)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding cached JSON: {e}")
            raise HTTPException(status_code=500, detail="Error processing cached data")

    # Retrieve an item from CouchDB by its document ID
    if item_id not in db:
        logger.info("Failed getting item by id")
        raise HTTPException(status_code=404, detail="Item not found")
    
    doc = db[item_id]
    item = models.Item(**doc)
    item.id = item_id  # Attach the document ID to the item, we need this because couchDB default is _id not the one in the model

    redis_client.setex(f"item:{item_id}", 60, item.model_dump_json())  # Cache for 60s to see somthing in the demo
    logger.info(f"Got item {item.name} with {item.id} and stored into cache.")
    return item

# GET - Get all items
@app.get("/items/", response_model=List[models.Item])
async def get_items():
    logger.info("GET items route called")
    # Retrieve all items from the CouchDB
    items = []
    for item_id in db:
        doc = db[item_id]
        # unpack it
        item = models.Item(**doc)
        item.id = item_id  # Attach the document ID to the item, since its None in the model
        items.append(item)
        
    logger.info("Served all items in the database.")
    return items

# PUT - Update one or more properties of an item by ID
@app.put("/items/{item_id}", response_model=dict)
async def update_item(item_id: str, item: models.Item):
    logger.info("PUT item route called")
    to_update = item.model_dump(exclude_unset=True)
    if app_settings.KAFKA_ENABLED == "true":
        event_message = {
            'action': 'update',
            'data': {
                'id': item_id,
                'fields': to_update
            }
        }
        kafka_service.send_message(app_settings.KAFKA_TOPIC, event_message)

        logger.info(f"Update request for item_id {item_id} sent to Kafka topic {app_settings.KAFKA_TOPIC}")

        resObj = {
            "message": f"Update request sent to Kafka topic {app_settings.KAFKA_TOPIC}",
            "status_code": 200
        }
        return resObj

    else:
        if item_id not in db:
            logger.error("Error updating item")
            raise HTTPException(status_code=404, detail="Item not found")
        
        doc = db[item_id]
        doc.update(to_update)
        db[item_id] = doc
        item.id = item_id
        logger.info(f"Item {item.name} with {item.id} updated.")
        return item

# DELETE - Delete an item by ID
@app.delete("/items/{item_id}", status_code=204)
async def delete_item(item_id: str):
    logger.info("DELETE item route called")
    if app_settings.KAFKA_ENABLED == "true":
        event_message = {
            'action': 'delete',
            'data': {
                'id': item_id
            }
        }
        kafka_service.send_message(app_settings.KAFKA_TOPIC, event_message)

        logger.info(f"Delete request for item_id {item_id} sent to Kafka topic {app_settings.KAFKA_TOPIC}")

        resObj = {
            "message": f"Delete request sent to Kafka topic {app_settings.KAFKA_TOPIC}",
            "status_code": 200
        }
        return resObj

    else:
        if item_id not in db:
            raise HTTPException(status_code=404, detail="Item not found")
        
        del db[item_id]
        logger.info(f"Item with {item_id} deleted.")
        return {"message": "Item deleted successfully"}
