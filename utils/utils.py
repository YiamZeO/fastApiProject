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


def date_condition_from_segment(segment):
    date_from = '%(date_from)s'
    date_to = '%(date_to)s'
    match segment:
        case 'weekofyear':
            date_from = f'toStartOfWeek(cast({date_from} as Date), 1)'
            date_to = f'toStartOfWeek(cast({date_to} as Date), 1)'
        case 'month':
            date_from = f'toStartOfMonth(cast({date_from} as Date))'
            date_to = f'toStartOfMonth(cast({date_to} as Date))'
        case 'year':
            date_from = f'toStartOfYear(cast({date_from} as Date))'
            date_to = f'toStartOfYear(cast({date_to} as Date))'
    return f'(from_dt between {date_from} and {date_to})'
