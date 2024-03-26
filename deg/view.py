from typing import List

from fastapi import APIRouter

from deg.model import DegWidgetModel, DegType

deg_widgets_router = APIRouter(
    prefix='/deg_widgets',
    tags=['deg_widgets']
)


@deg_widgets_router.get('/', response_model=List[DegWidgetModel], response_model_by_alias=False)
async def get_list(
        type: DegType | None = None,
        schema: str | None = None
):
    filter = list()
    if type:
        filter.append(DegWidgetModel.type == type)
    if schema:
        filter.append(DegWidgetModel.deg_schema == schema)
    return await DegWidgetModel.find(*filter).to_list()


@deg_widgets_router.get('/{id}', response_model=DegWidgetModel | None, response_model_by_alias=False)
async def get_by_id(id: str):
    return await DegWidgetModel.get(id)

@deg_widgets_router.post('/', response_model=DegWidgetModel, response_model_by_alias=False)
async def save(data: DegWidgetModel):
    return await data.save()

@deg_widgets_router.delete('/{id}', response_model=DegWidgetModel | None, response_model_by_alias=False)
async def get_by_id(id: str):
    d_widget = await DegWidgetModel.get(id)
    if d_widget:
        await d_widget.delete()
    return d_widget

