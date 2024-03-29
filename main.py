import logging

from fastapi import FastAPI

from deg.view import deg_widgets_router
from mesh.view import mesh_router
from settings import clickhouse_holder, mongo_beanie_init, mongo_beanie_close

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.include_router(deg_widgets_router)
app.include_router(mesh_router)


@app.on_event('startup')
async def startup():
    async with await clickhouse_holder.get_connection() as conn:
        async with conn.cursor() as cursor:
            logger.info(f'{clickhouse_holder.connection_pool} ClickHouse pool created')
    await mongo_beanie_init()


@app.on_event('shutdown')
async def shutdown():
    mongo_beanie_close()
    pool = str(clickhouse_holder.connection_pool)
    await clickhouse_holder.close_pool()
    logger.info(f'{pool} ClickHouse pool closed')
