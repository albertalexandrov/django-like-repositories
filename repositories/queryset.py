import logging
import random
from copy import deepcopy
from typing import Self, Any, Type

from sqlalchemy import Result, Row
from sqlalchemy.ext.asyncio import AsyncSession

from repositories.builder import QueryBuilder
from repositories.constants import LOOKUP_SEP
from repositories.types import Model
from repositories.utils import validate_has_columns, get_column, flush_or_commit

logger = logging.getLogger("repositories")


def iterate_scalars(result: Result) -> list[Model]:
    return list(result.scalars().all())


def iterate_flat_values_list(result: Result) -> list[Any]:
    return list(result.scalars().all())


def iterate_values_list(result: Result) -> list[tuple]:
    return list(tuple(item) for item in result.tuples().all())


def iterate_named_values_list(result: Result) -> list[Row]:
    return list(result.tuples().all())


class QuerySet:
    """

    """
    def __init__(self, model: Type[Model], session: AsyncSession):
        self._model_cls = model
        self._session = session
        self._query_builder = QueryBuilder(self._model_cls)
        self._iterable_result_func = iterate_scalars

    def _clone(self) -> Self:
        """
        Возвращает копию кверисета
        """
        # todo: проверить, что происходит с изменяемыми атрибутами при изменении этих атрибутов в копиях
        clone = self.__class__(self._model_cls, self._session)
        # делать копии сессии и модели не нужно
        clone._query_builder = self._query_builder.clone()
        return clone

    def filter(self, **kw: dict[str:Any]) -> Self:
        clone = self._clone()
        clone._query_builder.filter(**kw)
        return clone

    def order_by(self, *args: str) -> Self:
        clone = self._clone()
        clone._query_builder.order_by(*args)
        return clone

    def options(self, *args: str) -> Self:
        clone = self._clone()
        clone._query_builder.options(*args)
        return clone

    def innerjoin(self, *args: str) -> Self:
        clone = self._clone()
        clone._query_builder.join(*args, isouter=False)
        return clone

    def outerjoin(self, *args: str) -> Self:
        clone = self._clone()
        clone._query_builder.join(*args, isouter=True)
        return clone

    def execution_options(self, **kw: dict[str:Any]) -> Self:
        clone = self._clone()
        clone._query_builder.execution_options(**kw)
        return clone

    def returning(self, *args: str, return_model: bool = False) -> Self:
        clone = self._clone()
        clone._query_builder.returning(*args, return_model=return_model)
        return clone

    def values_list(self, *args: str, flat: bool = False, named: bool = False) -> Self:
        if flat and named:
            raise TypeError("'flat' и 'named' не могут быть заданы одновременно")
        if flat and len(args) > 1:
            raise TypeError("'flat' не валиден, когда метод values_list() вызывается с более чем одним полем")
        clone = self._clone()
        clone._query_builder.values_list(*args)
        clone._iterable_result_func = (
            iterate_named_values_list
            if named
            else iterate_flat_values_list if flat else iterate_values_list
        )
        return clone

    def limit(self, limit: int | None) -> Self:
        clone = self._clone()
        clone._query_builder.limit(limit)
        return clone

    def offset(self, offset: int | None) -> Self:
        clone = self._clone()
        clone._query_builder.offset(offset)
        return clone

    def distinct(self):
        clone = self._clone()
        clone._query_builder.distinct()
        return clone

    async def all(self) -> list[Any]:
        stmt = self._query_builder.build_select_stmt()
        result = await self._session.execute(stmt)
        # SQLAlchemy требует вызвать метод unique
        # The unique() method must be invoked on this Result, as it contains results
        # that include joined eager loads against collections
        return self._iterable_result_func(result.unique())

    async def first(self) -> Model | None:
        stmt = self.limit(1)._query_builder.build_select_stmt()
        return await self._session.scalar(stmt)

    async def count(self) -> int:
        stmt = self._query_builder.build_count_stmt()
        return await self._session.scalar(stmt)

    async def get_one_or_none(self) -> Model | None:
        stmt = self.limit(2)._query_builder.build_select_stmt()
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def get_or_create(
        self, defaults: dict = None, *, flush: bool = False, commit: bool = False, **kwargs
    ) -> tuple[Model, bool]:
        if obj := await self.filter(**kwargs).get_one_or_none():
            return obj, False
        params = self._extract_model_params(defaults, **kwargs)
        obj = self._model_cls(**params)
        self._session.add(obj)
        await flush_or_commit(obj, session=self._session, flush=flush, commit=commit)
        return obj, True

    async def update_or_create(
        self, defaults=None, create_defaults=None, *, flush: bool = False, commit: bool = False, **kwargs
    ) -> tuple[Model, bool]:
        update_defaults = defaults or {}
        if create_defaults is None:
            create_defaults = update_defaults
        obj, created = await self.get_or_create(defaults=create_defaults, flush=flush, commit=commit, **kwargs)
        if created:
            return obj, False
        validate_has_columns(obj, *update_defaults.keys())
        obj.update(**update_defaults)
        await flush_or_commit(obj, session=self._session, flush=flush, commit=commit)
        return obj, created

    async def in_bulk(self, id_list: list[Any] = None, *, field_name="id") -> dict[Any:Model]:
        filters = {}
        validate_has_columns(self._model_cls, field_name)
        column = get_column(self._model_cls, field_name)
        if not column.primary_key or not column.unique:
            logger.warning(
                f"Поле `{field_name}` не является уникальным полем модели {self._model_cls.__name__}. "
                "Результат выполнения метода in_bulk() может быть неожидаемым"
            )
        if id_list is not None:
            if not id_list:
                return {}
            filter_key = "{}__in".format(field_name)
            id_list = tuple(id_list)
            filters[filter_key] = id_list
        objs = await self.filter(**filters).all()
        return {getattr(obj, field_name): obj for obj in objs}

    async def exists(self) -> bool:
        return await self.count() > 0

    async def delete(self, flush: bool = False, commit: bool = False) -> Result[Model]:
        stmt = self._query_builder.build_delete_stmt()
        result = await self._session.execute(stmt)
        await flush_or_commit(session=self._session, flush=flush, commit=commit)
        return result

    async def update(self, values: dict[str:Any], flush: bool = False, commit: bool = False) -> Result[Model]:
        stmt = self._query_builder.build_update_stmt(values)
        result = await self._session.execute(stmt)
        await flush_or_commit(session=self._session, flush=flush, commit=commit)
        return result

    def _extract_model_params(self, defaults: dict | None, **kwargs: dict[str:Any]) -> dict[str:Any]:
        defaults = defaults or {}
        params = {k: v for k, v in kwargs.items() if LOOKUP_SEP not in k}
        params.update(defaults)
        validate_has_columns(self._model_cls, *params.keys())
        return params
