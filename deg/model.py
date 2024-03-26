from enum import Enum
from typing import Annotated

from beanie import Document
from pydantic import BaseModel, Field


class DegType(Enum):
    EXPORT = 'EXPORT'
    BOARD = 'BOARD'


class DegField(BaseModel):
    name: str
    russian_name: Annotated[str | None, Field(alias='russianName')] = None
    show: bool
    order: int


class AdditionalExport(BaseModel):
    first: str
    second: str
    db_column_name: str


class DegWidgetModel(Document):
    alias: str | None = None
    deg_schema: Annotated[str, Field(alias='deg')]
    order: int
    russian_name: Annotated[str | None, Field(alias='russianName')] = None
    show: bool
    table_name: str
    type: DegType
    fields_list: list[DegField]
    additional_export: AdditionalExport | None = None

    class Settings:
        name = 'deg_widget'
        validate_on_save = True
        keep_nulls = False

    model_config = dict(Document.model_config)
    model_config['json_schema_extra'] = {
        "example": {
            "id": "string",
            "alias": "string",
            "deg_schema": "string",
            "order": 0,
            "russian_name": "string",
            "show": True,
            "table_name": "string",
            "type": "EXPORT",
            "fields_list": [
                {
                    "name": "string",
                    "russian_name": "string",
                    "show": True,
                    "order": 0
                }
            ],
            "additional_export": {
                "first": "string",
                "second": "string",
                "db_column_name": "string"
            }
        }
    }
