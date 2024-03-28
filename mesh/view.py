from datetime import date

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


@mesh_router.get('/os/', response_model=ResponseObject)
async def get_os_data():
    return await MeshService.get_os_data()


@mesh_router.get('/devices/', response_model=ResponseObject)
async def get_devices_data():
    return await MeshService.get_devices_data()


@mesh_router.get('/visits/', response_model=ResponseObject)
async def get_visits_data(
        product: str,
        segment: str,
        date_from: date,
        date_to: date
):
    return await MeshService.get_visits_data(product, segment, date_from, date_to)


@mesh_router.get('/uniq_users/', response_model=ResponseObject)
async def get_uniq_users_data(
        product: str,
        segment: str,
        date_from: date,
        date_to: date
):
    return await MeshService.get_uniq_users_data(product, segment, date_from, date_to)
