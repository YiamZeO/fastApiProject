import logging

import asynch
from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

from deg.model import DegWidgetModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mongodb_beanie_client = AsyncIOMotorClient('mongodb://stats:stats@localhost:27017/stats')


async def mongo_beanie_init():
    await init_beanie(database=mongodb_beanie_client['stats'], document_models=[DegWidgetModel, ])
    logger.info(f'{mongodb_beanie_client} Beanie initialized')


async def mongo_beanie_close():
    mongodb_beanie_client.close()
    logger.info(f'{mongodb_beanie_client} Beanie closed')


class ClickHouseHolder:
    def __init__(self, host, port, database='default', user=None, password=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.__connection_pool = None

    @property
    async def connection_pool(self):
        return self.__connection_pool

    async def get_connection(self):
        if not self.__connection_pool:
            config = {'host': self.host, 'port': self.port, 'database': self.database}
            if self.user:
                config['user'] = self.user
            if self.password:
                config['password'] = self.password
            self.__connection_pool = await asynch.create_pool(**config)
        return self.__connection_pool.acquire()

    async def close_pool(self):
        if self.__connection_pool:
            self.__connection_pool.close()
            await self.__connection_pool.wait_closed()
        self.__connection_pool = None


clickhouse_holder = ClickHouseHolder('127.0.0.1', 9000)
