from fastapi_filter.contrib.sqlalchemy import Filter

from models import Section

# todo: кастомный базовый класс фильтрации от Сережи

class SectionFilters(Filter):
    name__ilike: str = None
    ordering: list[str] | None = ["id"]

    class Constants(Filter.Constants):
        model = Section
        ordering_field_name = "ordering"
