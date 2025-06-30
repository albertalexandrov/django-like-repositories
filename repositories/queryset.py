import logging
from typing import Self, Any, Type

from sqlalchemy import Result, Row
from sqlalchemy.ext.asyncio import AsyncSession

from repositories.builder import QueryBuilder
from repositories.constants import LOOKUP_SEP
from repositories.types import Model
from repositories.utils import validate_has_columns, get_column

logger = logging.getLogger("repositories")


def iterate_scalars(result: Result) -> list[Model]:
    return list(result.scalars().all())


def iterate_values_list(result: Result) -> list[tuple]:
    return list(tuple(item) for item in result.tuples().all())


def iterate_named_values_list(result: Result) -> list[Row]:
    return list(result.tuples().all())


class QuerySet:
    """
    Представляет собой ленивый поиск в базе данных для набора объектов (во многом аналог Django QuerySet)

    - ПРОМЕЖУТОЧНЫЕ И ТЕРМИНАЛЬНЫЕ МЕТОДЫ

    Класс содержит методы, которые деляться на два типа:
        1. промежуточные и
        2. терминальные.

    Промежуточные методы - filter(), order_by(), returning(), innerjoin(), outerjoin(), options(),
    execution_options(), values_list(), distinct(), flush(), commit()) - не выполняют запросов в БД, а
    предназначены для того, чтобы принимать в себя параметры запроса (параметры фильтрации, сортировки и тд)
    Промежуточные методы возвращают копию QuerySet.

    Терминальные методы - first(), count(), get_one_or_none(), delete(), update(), exists(), in_bulk(),
    update_or_create(), get_or_create() - соответственно, выполняют запросы в БД.

    - ВЫЧИСЛЕНИЕ QuerySet

    Вычисляется QuerySet простым await-ом:

        >>> qs = some_repository.object.filter(status_code="published")
        >>> result = await qs

    - СРЕЗЫ

    Лимитировать QuerySet можно при помощи срезов (шаг среза не поддерживается).  Для этого необходимо
    передать срез:

        >>> qs = some_repository.object.filter(status_code="published")[10:20]
        >>> result = await qs

    Это добавит в итоговый запрос LIMIT и OFFSET. Также возможно задать индекс:

        >>> qs = some_repository.object.filter(status_code="published")[0]
        >>> obj = await qs

    И тогда это вернет объект, а не список

    - УПРАВЛЕНИЕ ЖИЗНЕННЫМ ЦИКЛОМ СЕССИИ SQLAlchemy

    Иногда необходимо выполнить flush или commit после выполнения запроса или, напр., для получения id
    вновь созданного объекта (для этого выполняется flush).  Для этого необходимо дать инструкции при
    помощих соответствующих методов flush() и commit():

        >>> await some_repository.object.filter(status_code="published").commit().delete()

    Параметры управления жизненным циклом сессии определяются для каждого запроса

    - КЭШИРОВАНИЕ

    Результат вычисления QuerySet не кэшируется.
    """
    def __init__(self, model: Type[Model], session: AsyncSession):
        self._model_cls = model
        self._session = session
        self._query_builder = QueryBuilder(self._model_cls)
        self._iterate_result_func = iterate_scalars
        self._flush = False
        self._commit = False
        self._scalar = False
        self._sliced = False

    def _clone(self) -> Self:
        """
        Возвращает копию QuerySet
        """
        clone = self.__class__(self._model_cls, self._session)
        clone._query_builder = self._query_builder.clone()
        clone._flush = self._flush
        clone._commit = self._commit
        clone._scalar = self._scalar
        clone._sliced = self._sliced
        return clone

    def filter(self, **kw: dict[str:Any]) -> Self:
        self._validate_sliced()
        clone = self._clone()
        clone._query_builder.filter(**kw)
        return clone

    def order_by(self, *args: str) -> Self:
        self._validate_sliced()
        clone = self._clone()
        clone._query_builder.order_by(*args)
        return clone

    def options(self, *args: str) -> Self:
        self._validate_sliced()
        clone = self._clone()
        clone._query_builder.options(*args)
        return clone

    def innerjoin(self, *args: str) -> Self:
        self._validate_sliced()
        clone = self._clone()
        clone._query_builder.join(*args, isouter=False)
        return clone

    def outerjoin(self, *args: str) -> Self:
        self._validate_sliced()
        clone = self._clone()
        clone._query_builder.join(*args, isouter=True)
        return clone

    def execution_options(self, **kw: dict[str:Any]) -> Self:
        self._validate_sliced()
        clone = self._clone()
        clone._query_builder.execution_options(**kw)
        return clone

    def returning(self, *args: str, return_model: bool = False) -> Self:
        self._validate_sliced()
        clone = self._clone()
        clone._query_builder.returning(*args, return_model=return_model)
        return clone

    def flush(self, flush: bool = True, /) -> Self:
        clone = self._clone()
        clone._flush = flush
        return clone

    def commit(self, commit: bool = True, /) -> Self:
        clone = self._clone()
        clone._commit = commit
        return clone

    async def _flush_commit_reset(self, *objs: Model) -> None:
        if self._flush and not self._commit and objs:
            await self._session.flush(objs)
        elif self._commit:
            await self._session.commit()
        # основная идея сброса параметров управления жизненным циклом сессии SQLAlchemy состоит в том,
        # чтобы в при каждом запрос явно им управлять
        self._flush = None
        self._commit = None

    def values_list(self, *args: str, flat: bool = False, named: bool = False) -> Self:
        if flat and named:
            raise TypeError("'flat' и 'named' не могут быть заданы одновременно")
        if flat and len(args) > 1:
            raise TypeError("'flat' не валиден, когда метод values_list() вызывается с более чем одним полем")
        clone = self._clone()
        clone._query_builder.values_list(*args)
        clone._iterate_result_func = (
            iterate_named_values_list
            if named
            else iterate_scalars if flat else iterate_values_list
        )
        return clone

    def distinct(self) -> Self:
        self._validate_sliced()
        clone = self._clone()
        clone._query_builder.distinct()
        return clone

    def all(self) -> Self:
        return self._clone()

    async def first(self) -> Model | None:
        return await self[0]

    async def count(self) -> int:
        stmt = self._query_builder.build_count_stmt()
        return await self._session.scalar(stmt)

    async def get_one_or_none(self) -> Model | None:
        stmt = self[:2]._query_builder.build_select_stmt()
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def get_or_create(self, defaults: dict = None, **kw) -> tuple[Model, bool]:
        if obj := await self.filter(**kw).get_one_or_none():
            return obj, False
        params = self._extract_model_params(defaults, **kw)
        obj = self._model_cls(**params)
        self._session.add(obj)
        await self._flush_commit_reset(obj)
        return obj, True

    async def update_or_create(self, defaults=None, create_defaults=None, **kw) -> tuple[Model, bool]:
        update_defaults = defaults or {}
        if create_defaults is None:
            create_defaults = update_defaults
        obj, created = await self.get_or_create(defaults=create_defaults, **kw)
        if created:
            return obj, False
        validate_has_columns(obj.__class__, *update_defaults.keys())
        obj.update(**update_defaults)
        await self._flush_commit_reset(obj)
        return obj, created

    async def in_bulk(self, id_list: list[Any] = None, *, field_name="id") -> dict[Any:Model]:
        self._validate_sliced()
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
        objs = await self.filter(**filters)
        return {getattr(obj, field_name): obj for obj in objs}

    async def exists(self) -> bool:
        return await self.count() > 0

    async def delete(self) -> Result[Model]:
        self._validate_sliced()
        stmt = self._query_builder.build_delete_stmt()
        result = await self._session.execute(stmt)
        await self._flush_commit_reset()
        return result

    async def update(self, values: dict[str:Any]) -> Result[Model]:
        self._validate_sliced()
        stmt = self._query_builder.build_update_stmt(values)
        result = await self._session.execute(stmt)
        await self._flush_commit_reset()
        return result

    def _extract_model_params(self, defaults: dict | None, **kw: dict[str:Any]) -> dict[str:Any]:
        defaults = defaults or {}
        params = {k: v for k, v in kw.items() if LOOKUP_SEP not in k}
        params.update(defaults)
        validate_has_columns(self._model_cls, *params.keys())
        return params

    def __await__(self) -> list[Any]:
        stmt = self._query_builder.build_select_stmt()
        if self._scalar:
            obj = yield from self._session.scalar(stmt).__await__()
            return obj
        result = yield from self._session.execute(stmt).__await__()
        # SQLAlchemy требует вызвать метод unique(), иначе выдает ошибку:
        #   The unique() method must be invoked on this Result, as it contains results
        #   that include joined eager loads against collections
        return self._iterate_result_func(result.unique())

    def __getitem__(self, k: int | slice) -> Self:
        self._validate_sliced()
        if not isinstance(k, (int, slice)):
            raise TypeError(
                "Индекс должен быть целыми числом или объектом slice, а не %s."
                % type(k).__name__
            )
        if (isinstance(k, int) and k < 0) or (isinstance(k, slice) and ((k.start is not None and k.start < 0) or (k.stop is not None and k.stop < 0))):
            raise ValueError("Отрицательные индексы не поддерживаются")
        if isinstance(k, slice):
            if k.step is not None:
                raise ValueError("Использование шага среза не предусмотрена")
            elif k.start >= k.stop:
                raise ValueError("Начало срез должно быть меньше конца")
        clone = self._clone()
        if isinstance(k, int):
            clone._scalar = True
            clone._query_builder.limit(1)
            clone._query_builder.offset(k)
        else:
            clone._scalar = False
            clone._query_builder.limit(k.stop - k.start)
            clone._query_builder.offset(k.start)
        self._sliced = True
        return clone

    def _validate_sliced(self) -> None:
        """
        Принимаем, что если был взят срез, то после невозможно менять QuerySet

        Внятной мотивации для этого нет.  Просто кажется, что такая, например,
        цепочка вызовов методов выглядит более чем странной:

            >>> some_repository.objects.filter(code="code")[:3].order_by("id")[0][0:1]

        Можно сказать, что это предтерминальный метод
        """
        if self._sliced:
            raise TypeError("Невозможно изменить запрос после того, как срез был взят.")
