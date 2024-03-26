from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

from deg.model import DegWidgetModel

mongodb_client: AsyncIOMotorClient | None = None


async def get_mongodb_client():
    global mongodb_client
    if not mongodb_client:
        mongodb_client = AsyncIOMotorClient('mongodb://stats:stats@localhost:27017/stats')
        await init_beanie(database=mongodb_client['stats'], document_models=[DegWidgetModel])
    return mongodb_client


async def close_mongodb_client():
    global mongodb_client
    if mongodb_client:
        mongodb_client.close()
