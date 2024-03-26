from fastapi import FastAPI

from deg.view import deg_widgets_router
from settings import close_mongodb_client, get_mongodb_client
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.include_router(deg_widgets_router)

@app.on_event('startup')
async def startup():
    logger.info(str(await get_mongodb_client()) + ' client opened')

@app.on_event('shutdown')
async def shutdown():
    logger.info(str(await get_mongodb_client()) + ' client closing')
    await close_mongodb_client()
