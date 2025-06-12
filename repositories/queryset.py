import logging
from copy import deepcopy
from typing import Self, TypeVar, Any, Generator

from sqlalchemy import Result, RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from repositories.builder import QueryBuilder
from repositories.constants import LOOKUP_SEP
from repositories.exceptions import MultipleObjectsReturnedError
from repositories.utils import validate_has_columns, get_column

Model = TypeVar("Model")
logger = logging.getLogger("repositories")


def iterate_scalars(result: Result) -> list[Model]:
    return list(result.scalars().all())


def iterate_flat_values_list(result: Result) -> list[Any]:
    return list(result.scalars().all())


def iterate_values_list(result: Result) -> list[RowMapping]:
    # t = result.tuples().all()
    return list(item._data for item in result.tuples().all())


def iterate_named_values_list(result: Result):
    return list(result.tuples().all())


class QuerySet:
    def __init__(self, model, session: AsyncSession):
        self._model_cls = model
        self._session = session
        self._query_builder = QueryBuilder(self._model_cls)
        self._iterable_result_func = iterate_scalars

    def _clone(self) -> Self:
        # todo: проверить, что происходит с изменяемыми атрибутами при изменении этих атрибутов в копиях
        clone = self.__class__(self._model_cls, self._session)
        clone._model_cls = self._model_cls
        clone._session = self._session
        # делать копии сессии и модели не нужно
        clone._query_builder = deepcopy(self._query_builder)
        return clone

    def filter(self, **kwargs) -> Self:
        """
        Обрабатывает условия фильтрации и попутно join-ы

        Фильтрация по экземплярам модели как в Django, когда, получив экземпляр связной модели, например,
        Status(...), Django преобразует его в status_id, видится невозможной или трудновыполнимой, тк в
        SQLAlchemy FK и соответствующий relationship задаются независимо друг от друга

        :param kwargs: фильтры
        """
        clone = self._clone()
        clone._query_builder.filter(**kwargs)
        return clone

    def options(self, *fields: str) -> Self:
        clone = self._clone()
        clone._query_builder.options(*fields)
        return clone

    def order_by(self, *args: str) -> Self:
        clone = self._clone()
        clone._query_builder.order_by(*args)
        return clone

    async def all(self) -> list:
        stmt = self._query_builder.build_select_stmt()
        result = await self._session.execute(stmt)
        return self._iterable_result_func(result)

    async def first(self) -> Model | None:
        clone = self._clone()
        clone.limit(1)
        stmt = self._query_builder.build_select_stmt()
        return await self._session.scalar(stmt)

    def outerjoin(self, *args: str) -> Self:
        clone = self._clone()
        self._query_builder.join(*args, isouter=True)
        return clone

    def innerjoin(self, *args: str) -> Self:
        clone = self._clone()
        self._query_builder.join(*args, isouter=False)
        return clone

    def limit(self, limit: int) -> Self:
        clone = self._clone()
        clone._query_builder.limit(limit)
        return clone

    def offset(self, offset: int) -> Self:
        clone = self._clone()
        clone._query_builder.offset(offset)
        return clone

    async def count(self) -> int:
        clone = self._clone()
        stmt = clone._query_builder.build_count_stmt()
        return await clone._session.scalar(stmt)

    async def get_one_or_none(self) -> Model | None:
        objs = await self.limit(2).all()
        if len(objs) > 1:
            raise MultipleObjectsReturnedError
        return objs[0] if objs else None

    async def get_or_create(self, defaults=None, **kwargs) -> tuple[Model, bool]:
        # todo: параметры flush, commit?
        if obj := await self.filter(**kwargs).get_one_or_none():
            return obj, False
        params = self._extract_model_params(defaults, **kwargs)
        instance = self._model_cls(**params)
        self._session.add(instance)
        await self._session.flush([instance])
        return instance, True

    def _extract_model_params(self, defaults: dict | None, **kwargs: dict[str:Any]) -> dict[str:Any]:
        defaults = defaults or {}
        params = {k: v for k, v in kwargs.items() if LOOKUP_SEP not in k}
        params.update(defaults)
        validate_has_columns(self._model_cls, *params.keys())
        return params

    async def update_or_create(self, defaults=None, create_defaults=None, **kwargs) -> tuple[Model, bool]:
        # todo: параметры flush, commit?
        update_defaults = defaults or {}
        if create_defaults is None:
            create_defaults = update_defaults
        obj, created = await self.get_or_create(defaults=create_defaults, **kwargs)
        if created:
            return obj, created
        validate_has_columns(obj, *update_defaults.keys())
        obj.update(**update_defaults)
        return obj, False

    # note: bulk_update как по мне не нужен

    async def bulk_create(self, values: list[dict[str, Any]]) -> list:
        # todo: параметры flush, commit?
        instances = [self._model_cls(**item) for item in values]
        self._session.add_all(instances)
        await self._session.flush([instances])
        return instances

    async def in_bulk(self, id_list=None, *, field_name="id") -> dict[Any:Model]:
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

    async def delete(self) -> Result[Model]:
        stmt = self._query_builder.build_delete_stmt()
        return await self._session.execute(stmt)

    def execution_options(self, **execution_options) -> Self:
        clone = self._clone()
        clone._query_builder.execution_options(**execution_options)
        return clone

    def returning(self, *cols, return_model: bool = False) -> Self:
        """
        Добавляет RETURNING к запросу

        Учитывается при запросах UPDATE и DELETE

        :param cols: названия столбцов
        :param return_model: инструкция вернуть все столбцы модели
        """
        clone = self._clone()
        clone._query_builder.returning(*cols, return_model=return_model)
        return clone

    async def update(self, **values) -> Result[Model]:
        stmt = self._query_builder.build_update_stmt(values)
        return await self._session.execute(stmt)

    def values_list(self, *args, flat: bool = False, named: bool = False) -> Self:
        if flat and named:
            raise TypeError("'flat' и 'named' не могут быть использованы вместе")
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
