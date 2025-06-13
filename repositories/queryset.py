import logging
from copy import deepcopy
from typing import Self, TypeVar, Any

from sqlalchemy import Result, RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from repositories.builder import QueryBuilder
from repositories.constants import LOOKUP_SEP
from repositories.utils import validate_has_columns, get_column

Model = TypeVar("Model")
logger = logging.getLogger("repositories")


def iterate_scalars(result: Result) -> list[Model]:
    return list(result.scalars().all())


def iterate_flat_values_list(result: Result) -> list[Any]:
    return list(result.scalars().all())


def iterate_values_list(result: Result) -> list[RowMapping]:
    return list(item._data for item in result.tuples().all())


def iterate_named_values_list(result: Result):
    return list(result.tuples().all())


class QuerySet:
    def __init__(self, model: Model, session: AsyncSession):
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
        clone._model_cls = self._model_cls
        clone._session = self._session
        # делать копии сессии и модели не нужно
        clone._query_builder = deepcopy(self._query_builder)
        return clone

    def filter(self, **kw) -> Self:
        """
        Сохраняет условия фильтрации

        Промежуточный метод - возвращает новый QuerySet

        Аналог метода QuerySet.filter() в Django

        Примеры фильтров:
            - name__ilike (фильтрация по полю модели с lookup-ом)
            - status__code (фильтрация по полю связной модели)

        :param kw: условия фильтрации
        """
        clone = self._clone()
        clone._query_builder.filter(**kw)
        return clone

    def options(self, *args: str) -> Self:
        """
        Добавляет options в итоговый запрос

        Промежуточный метод - возвращает новый QuerySet

        Примеры options:
            - status (подгрузить экземпляры связной модели Status)
            - subsections__article_contents (подгрузить экземпляры связных моделей Subsection и ArticleContent)

        :param args: перечисленные через запятую options
        """
        clone = self._clone()
        clone._query_builder.options(*args)
        return clone

    def order_by(self, *args: str) -> Self:
        """
        Добавляет условия сортировки в итоговый запрос

        Промежуточный метод - возвращает новый QuerySet

        Примеры сортировки:
            - status_id (простая сортировка)
            - status__code (сортировка по полю связной модели)

        :param args: перечисленные через запятую поля и направления сортировки
        """
        clone = self._clone()
        clone._query_builder.order_by(*args)
        return clone

    def outerjoin(self, *args: str) -> Self:
        """
        Добавляет OUTER JOIN в итоговый запрос

        Промежуточный метод - возвращает новый QuerySet

        Внешним join-ом станет только последний join.  Например, при указании
        subsection__article_content, внешним join-ом будет сохранен только article_content

        Необходимость метода обусловлена тем, что по умолчанию связные модели join-ятся при помощи
        INNER JOIN, и бывает необходимо получить записи, у которых наоборот отсутствуют связные
        записи

        :param args: перечисленные через запятую join-ы
        """
        clone = self._clone()
        self._query_builder.join(*args, isouter=True)
        return clone

    def innerjoin(self, *args: str) -> Self:
        """
        Добавляет INNER JOIN к итоговому запросу

        Промежуточный метод - возвращает новый QuerySet

        Внутренным join-ом станет только последний join.  Например, при указании
        subsection__article_content, внутренным join-ом будет сохранен только article_content

        Необходимость метода обусловлена необходимостью изменить тип join-а в случаях, когда определяется
        option.  Допустим, необходимо подгрузить экземпляры связной модели.  Это будет сделано при помощи
        joinedload, который по умолчанию выполняет OUTER JOIN.

        :param args: перечисленные через запятую строковые названия join-ов
        """
        clone = self._clone()
        self._query_builder.join(*args, isouter=False)
        return clone

    def execution_options(self, **kw) -> Self:
        """
        Добавляет execution_options в итоговый запрос

        Промежуточный метод - возвращает новый QuerySet

        См https://docs.sqlalchemy.org/en/20/core/selectable.html#sqlalchemy.sql.expression.Select.execution_options

        Повторный вызов метода перезапишет ранее сохраненные значения

        :param kw: параметры запроса
        """
        clone = self._clone()
        clone._query_builder.execution_options(**kw)
        return clone

    def returning(self, *args, return_model: bool = False) -> Self:
        """
        Добавляет returning к итоговому запросу

        Промежуточный метод - возвращает новый QuerySet

        Повторный вызов метода перезапишет ранее сохраненные значения

        Учитывается при запросах UPDATE и DELETE

        :param args: названия столбцов
        :param return_model: признак необходимости вернуть все столбцы модели
        """
        clone = self._clone()
        clone._query_builder.returning(*args, return_model=return_model)
        return clone

    def values_list(self, *args, flat: bool = False, named: bool = False) -> Self:
        """
        Добавляет столбцы для выборки в итоговый запрос

        Промежуточный метод - возвращает новый QuerySet

        По умолчанию возвращается список коржетей.  Поведение можно изменить параметрами flat и named

        Невозможно запросить поля связных моделей

        :param args: названия столбцов
        :param flat: признак необходимости вернуть плоский список значений, когда запрашивается одно поле
        :param named: признак необходимости вернуть список именованных кортежей
        """
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

    def limit(self, limit: int) -> Self:
        clone = self._clone()
        clone._query_builder.limit(limit)
        return clone

    def offset(self, offset: int) -> Self:
        clone = self._clone()
        clone._query_builder.offset(offset)
        return clone

    async def all(self) -> list[Any]:
        """
        Выполняет запрос в БД и применяет к его результату функцию _iterable_result_func

        Терминальный метод - выполняет запрос в БД
        """
        stmt = self._query_builder.build_select_stmt()
        result = await self._session.execute(stmt)
        return self._iterable_result_func(result)

    async def first(self) -> Model | None:
        """
        Возвращает первую запись

        Терминальный метод - выполняет запрос в БД
        """
        stmt = self.limit(1)._query_builder.build_select_stmt()
        return await self._session.scalar(stmt)

    async def count(self) -> int:
        """
        Выполняет подсчет количества записей

        Терминальный метод - выполняет запрос в БД
        """
        stmt = self._query_builder.build_count_stmt()
        return await self._session.scalar(stmt)

    async def get_one_or_none(self) -> Model | None:
        """
        Возвращает один экземпляр или None

        Терминальный метод - выполняет запрос в БД
        """
        stmt = self.limit(2)._query_builder.build_select_stmt()
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def get_or_create(
        self, defaults: dict = None, *, flush: bool = False, commit: bool = False, **kwargs
    ) -> tuple[Model, bool]:
        """
        Выполняет поиск существующей записи по заданным kwargs, и создает новую запись, если запись не найдена

        Терминальный метод - выполняет запросы

        Метод возвращает кортеж (obj, created), где obj — это найденный или созданный экземпляр
        модели, а created — булевая переменная, указывающая на создание объекта (True) или его
        извлечение из базы данных (False)

        Если поле в kwargs совпадает названием с defaults, flush bли commit, то этому полю необходимо добавить
        lookup exact, то есть defaults__exact и тд, и при необходимости добавить его в defaults, чтобы его
        значение было сохранено при создании нового экземпляра

        :param defaults: значения для создания экземпляра
        :param flush: признак необходимости выполнить flush
        :param commit: признак необходимости выполнить сommit
        :param kwargs: условия поиска существующей записи
        """
        if obj := await self.filter(**kwargs).get_one_or_none():
            return obj, False
        params = self._extract_model_params(defaults, **kwargs)
        obj = self._model_cls(**params)
        self._session.add(obj)
        await self._flush_or_commit(obj, flush=flush, commit=commit)
        return obj, True

    async def update_or_create(
        self, defaults=None, create_defaults=None, *, flush: bool = False, commit: bool = False, **kwargs
    ) -> tuple[Model, bool]:
        """
        Обновляет новую запись или обновляет существующую

        Терминальный метод - выполняет запросы

        :param defaults: значения для обновления существуюещей записи
        :param create_defaults: значения для создания новой записи
        :param flush: признак необходимости выполнить flush
        :param commit: признак необходимости выполнить сommit
        :param kwargs: условия поиска существующей записи
        """
        update_defaults = defaults or {}
        if create_defaults is None:
            create_defaults = update_defaults
        obj, created = await self.get_or_create(defaults=create_defaults, flush=flush, commit=commit, **kwargs)
        if created:
            return obj, False
        validate_has_columns(obj, *update_defaults.keys())
        obj.update(**update_defaults)
        if commit:
            await self._session.commit()
        return obj, created

    async def in_bulk(self, id_list: list[Any] = None, *, field_name="id") -> dict[Any:Model]:
        """
        Возвращает словарь, в экземпляр модели поставлен в соответствие значению поля field_name

        Терминальный метод - выполняет запросы

        :param id_list: список значений, по которым будут отфильтрованы записи
        :param field_name: название поля для фильтрации id_list
        """
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
        """
        Проверяет, существуют ли записи

        Терминальный метод - выполняет запросы
        """
        return await self.count() > 0

    async def delete(self) -> Result[Model]:
        """
        Выполняет удаление записей, удовлетворяющих заданным условиям

        Терминальный метод - выполняет запросы

        Если необходимо, чтобы запрос DELETE что-то вернул, то необходимо вызвать метод QuerySet.returning()
        """
        stmt = self._query_builder.build_delete_stmt()
        return await self._session.execute(stmt)

    async def update(self, **kw) -> Result[Model]:
        """
        Выполняет обновление записей, удовлетворяющих заданным условиям

        Терминальный метод - выполняет запросы

        Если необходимо, чтобы запрос DELETE что-то вернул, то необходимо вызвать метод QuerySet.returning()

        :param kw: поля и их новые значения
        """
        stmt = self._query_builder.build_update_stmt(kw)
        return await self._session.execute(stmt)

    async def _flush_or_commit(self, *objs: Model, flush: bool, commit: bool) -> None:
        """
        Выполняет flush или commit

        :param objs: экземпляры для flush
        :param flush: признак необходимости выполнить flush
        :param commit: признак необходимости выполнить сommit
        """
        if flush and not commit:
            await self._session.flush(objs)
        elif commit:
            await self._session.commit()

    def _extract_model_params(self, defaults: dict | None, **kwargs: dict[str:Any]) -> dict[str:Any]:
        defaults = defaults or {}
        params = {k: v for k, v in kwargs.items() if LOOKUP_SEP not in k}
        params.update(defaults)
        validate_has_columns(self._model_cls, *params.keys())
        return params
