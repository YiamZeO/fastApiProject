import io
from datetime import date, datetime
from typing import Annotated
from urllib.parse import quote

from fastapi import APIRouter, Query, Depends
from pydantic import BaseModel
from starlette.responses import StreamingResponse, FileResponse

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


@mesh_router.get('/geography/', response_model=ResponseObject)
async def get_geography_data(
        g_type: str,
        g_name: str | None = None,
):
    return await MeshService.get_geography_data(g_type, g_name)


class ReportFilter(BaseModel):
    category: str
    product: str | None = None
    segment: str | None = None
    date_from: date | None = None
    date_to: date | None = None
    g_name: str | None = None
    g_type: str | None = None


def validate_report_filter(report_filter):
    if report_filter.category == 'visits' or report_filter.category == 'uniq_users':
        return ReportFilter(category=report_filter.category, product=report_filter.product, segment=report_filter.segment,
                            date_from=report_filter.date_from, date_to=report_filter.date_to)
    elif report_filter.category == 'geography':
        return ReportFilter(category=report_filter.category, g_name=report_filter.g_name, g_type=report_filter.g_type)
    else:
        return ReportFilter(category=report_filter.category)


@mesh_router.get('/report/download/', response_class=StreamingResponse,
                 responses={200: {'content': {
                     'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': {'example': ''}}}})
async def report_download(report_filter: Annotated[ReportFilter, Depends()]):
    report_filter = validate_report_filter(report_filter)
    output = io.BytesIO()
    if report_filter.category in MeshService.categories_config_map:
        (await MeshService.report_download(report_filter.model_dump(exclude_none=True))).save(output)
        output.seek(0)
        file_name = (f'{MeshService.categories_config_map[report_filter.category].r_name.replace(' ', '_')}_'
                     f'{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx')
        response = StreamingResponse(output,
                                     media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = f'attachment; filename*=UTF-8\'\'{quote(file_name)}'
        return response
    else:
        return StreamingResponse(f'Категория {report_filter.category} не определена', media_type='text/plain')
