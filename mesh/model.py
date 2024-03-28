from typing import Any

from pydantic import BaseModel


class GeographyModel(BaseModel):
    district_name: str | None = None
    ao_name: str | None = None
    district_data: dict[str, Any] | None = None
    ao_data: dict[str, Any] | None = None

