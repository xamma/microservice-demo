from pydantic_settings import BaseSettings
from pydantic import BaseModel
from typing import List, Optional

"""
Configuration class for the app.
Precendences:
1. ENV Vars
2. .env File
3. Default Vars
"""

class AppSettings(BaseSettings):
    DB_PORT: int | None = 5984
    DB_PASSWORD: str | None = "password"
    DB_USERNAME: str | None = "admin"
    DB_HOST: str | None = "localhost"
    DB_NAME: str | None = "items_db"
    KAFKA_ENABLED: str | None = "false"
    KAFKA_BROKER: str | None = "localhost:9092"
    KAFKA_TOPIC: str | None = "items-topic"
    KAFKA_GROUP_ID: str | None = "test-consumer-group"
    REDIS_HOST: str | None = "localhost"
    REDIS_PORT: int | None = 6379
    REDIS_DB: int | None = 5

    class Config:
        env_file = ".env"

class Item(BaseModel):
    name: str
    description: Optional[str] = None
    price: float = 0.0
    tax: Optional[float] = None
    id: Optional[str] = None
