from typing import Annotated, Any

from pydantic import BaseModel, Field, ConfigDict


class ResponseObject(BaseModel):
    """
    Класс ответа для запросов:
    \n data -- данные
    \n meta -- мета данные
    """

    data: Any = None
    meta: Any = None
