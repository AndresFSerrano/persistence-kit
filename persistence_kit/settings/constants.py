from enum import Enum


class Database(str, Enum):
    MEMORY = "memory"
    MONGO = "mongo"
    POSTGRES = "postgres"
    DYNAMODB = "dynamodb"
