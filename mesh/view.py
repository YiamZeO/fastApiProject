from fastapi import APIRouter

from mesh.service import MeshService
from utils.utils import ResponseObject

mesh_router = APIRouter(
    prefix='/mesh',
    tags=['mesh']
)


@mesh_router.get('/date_ranges/', response_model=ResponseObject)
async def get_date_ranges(category: str):
    return await MeshService.get_date_ranges(category)


@mesh_router.get('/aos_and_districts/', response_model=ResponseObject)
async def get_aos_and_districts():
    return await MeshService.get_aos_and_districts()


@mesh_router.get('/ages/', response_model=ResponseObject)
async def get_ages_data():
    return await MeshService.get_ages_data()
