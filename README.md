# django-like-repositories

# Оглавление

- [Модели данных](#модели-данных)
- [Базовый репозиторий](#базовый-репозиторий)
- [QuerySet](#queryset)
  - [lookup-ы](#lookup-ы)
  - [Промежуточные методы](#промежуточные-методы)
  - [Терминальные методы](#терминальные-методы)
  - [Финальный сформированный запрос](#финальный-сформированный-запрос)
- [Примеры](#примеры)
  - [Простая фильтрация](#простая-фильтрация)
  - [Фильтрация по связной модели](#фильтрация-по-связной-модели)
  - [Простая сортировка](#простая-сортировка)
  - [Сортировка по полю связной модели](#сортировка-по-полю-связной-модели)
  - [options](#options)
  - [Получение Section, у которых отсутствуют связные Subsection](#получение-section-у-которых-отсутствуют-связные-subsection)
  - [Получение первой записи](#получение-первой-записи)
  - [Кастомный objects](#кастомный-objects)
- [Планы](#планы)

## Модели данных

Для примера возьмем модели данных из справки:

```python
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models import Base


class PublicationStatus(Base):
    __tablename__ = "publication_statuses"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(unique=True)
    name: Mapped[str]
    sections: Mapped[list["Section"]] = relationship(back_populates="status")
    subsections: Mapped[list["Subsection"]] = relationship(back_populates="status")


class Section(Base):
    __tablename__ = "sections"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    status_id: Mapped[int] = mapped_column(ForeignKey("publication_statuses.id"))
    status: Mapped["PublicationStatus"] = relationship(back_populates="sections")
    subsections: Mapped[list["Subsection"]] = relationship(back_populates="section")


class Subsection(Base):
    __tablename__ = "subsections"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    section_id: Mapped[int] = mapped_column(ForeignKey("sections.id"))
    section: Mapped["Section"] = relationship(back_populates="subsections")
    status_id: Mapped[int] = mapped_column(ForeignKey("publication_statuses.id"))
    status: Mapped["PublicationStatus"] = relationship(back_populates="subsections")
    article_contents: Mapped[list["ArticleContent"]] = relationship(
        back_populates="subsection"
    )


class Widget(Base):
    __tablename__ = "widgets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    code: Mapped[str] = mapped_column(unique=True)
    article_contents: Mapped[list["ArticleContent"]] = relationship(
        back_populates="widget"
    )


class ArticleContent(Base):
    __tablename__ = "article_contents"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subtitle: Mapped[str]
    text: Mapped[str]
    subsection_id: Mapped[int] = mapped_column(ForeignKey("subsections.id"))
    subsection: Mapped["Subsection"] = relationship(back_populates="article_contents")
    widget_id: Mapped[int] = mapped_column(ForeignKey("widgets.id"))
    widget: Mapped["Widget"] = relationship(back_populates="article_contents")
```

и их репозитории:

```python
from models import Section
from repositories.base import BaseRepository


class SectionRepository(BaseRepository):
    model = Section
```

## Базовый репозиторий

Плюс-минус стандартный:

```python
from typing import TypeVar, Generic, Any

from fastapi.params import Depends
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from dependencies import get_session
from repositories.queryset import QuerySet

Model = TypeVar("Model")


class BaseRepository(Generic[Model]):
    model: Model = None

    def __init__(self, session: AsyncSession = Depends(get_session)):
        self._session = session

    async def create(self, values: dict, flush: bool = True, commit: bool = False) -> Model:
        instance = self.model(**values)
        self._session.add(instance)
        if flush and not commit:
            await self._session.flush(instance)
        elif commit:
            await self._session.commit()
        return instance

    async def bulk_create(self, values: list[dict], flush: bool = True, commit: bool = False) -> list[Model]:
        objs = []
        for item in values:
            obj = self.model(**item)
            objs.append(obj)
        if flush and not commit:
            await self._session.flush(*objs)
        elif commit:
            await self._session.commit()
        return objs

    # методы с ограниченной функциональностью

    async def all(self) -> list[Model]:
        stmt = select(self.model)
        result = await self._session.scalars(stmt)
        return result.all()  # noqa

    async def get_by_pk(self, pk: Any) -> Model:
        return await self._session.get(self.model, pk)

    async def delete(self) -> None:
        stmt = delete(self.model)
        await self._session.execute(stmt)
        
    async def first(self) -> Model | None:
        stmt = select(self.model).limit(1)
        return await self._session.scalar(stmt)

    # возможно какие то другие методы с ограниченной фунциональностью (получение списка, напр.) - накидывайте

    @property
    def objects(self) -> QuerySet:
        return QuerySet(self.model, self._session)
```

Рассмотрим класс `QuerySet`.

## QuerySet

Поделка на QuerySet Django с некоторыми особенностями SQLAlchemy.

Пример использования:

```python
repository = SectionRepository(session)
queryset = (
    repository
    .objects
    .filter(name__icontains='управление')
    .filter(status__code='unpublished')
    .order_by('id')
    .options("status")
)
result = await queryset.all()
```

## Класс QuerySet

P.S. На момент написания этих строк реализованы `select`-методы `all`, `first`.

```python
from copy import deepcopy
from typing import Self

from fastapi_filter.contrib.sqlalchemy import Filter
from sqlalchemy import select, extract, inspect, Select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, contains_eager
from sqlalchemy.sql import operators


class QuerySet:
    _operators = {
        "in": operators.in_op,
        "isnull": lambda c, v: (c == None) if v else (c != None),
        "exact": operators.eq,
        "eq": operators.eq,
        "ne": operators.ne,
        "gt": operators.gt,
        "ge": operators.ge,
        "lt": operators.lt,
        "le": operators.le,
        "notin": operators.notin_op,
        "between": lambda c, v: c.between(v[0], v[1]),
        "like": operators.like_op,
        "ilike": operators.ilike_op,
        "startswith": operators.startswith_op,
        "istartswith": lambda c, v: c.ilike(v + "%"),
        "endswith": operators.endswith_op,
        "iendswith": lambda c, v: c.ilike("%" + v),
        "contains": lambda c, v: c.like(f"%{v}%"),
        "icontains": lambda c, v: c.ilike(f"%{v}%"),
        "year": lambda c, v: extract("year", c) == v,
        "year_ne": lambda c, v: extract("year", c) != v,
        "year_gt": lambda c, v: extract("year", c) > v,
        "year_ge": lambda c, v: extract("year", c) >= v,
        "year_lt": lambda c, v: extract("year", c) < v,
        "year_le": lambda c, v: extract("year", c) <= v,
        "month": lambda c, v: extract("month", c) == v,
        "month_ne": lambda c, v: extract("month", c) != v,
        "month_gt": lambda c, v: extract("month", c) > v,
        "month_ge": lambda c, v: extract("month", c) >= v,
        "month_lt": lambda c, v: extract("month", c) < v,
        "month_le": lambda c, v: extract("month", c) <= v,
        "day": lambda c, v: extract("day", c) == v,
        "day_ne": lambda c, v: extract("day", c) != v,
        "day_gt": lambda c, v: extract("day", c) > v,
        "day_ge": lambda c, v: extract("day", c) >= v,
        "day_lt": lambda c, v: extract("day", c) < v,
        "day_le": lambda c, v: extract("day", c) <= v,
    }

    def __init__(self, model, session: AsyncSession):
        self._model = model
        self._session = session
        self._stmt = select(self._model)
        self._options = {}
        self._where = set()
        self._joins = {}
        self._ordering_fields = set()
        self._limit = None
        self._offset = None

    def _clone(self):
        clone = self.__class__(self._model, self._session)
        clone._model = self._model
        clone._session = self._session
        clone._stmt = self._stmt
        clone._options = self._options
        clone._where = self._where
        clone._joins = self._joins
        clone._ordering_fields = self._ordering_fields
        clone._limit = self._limit
        clone._offset = self._offset
        return clone

    def filter(self, *, filtering: Filter = None, **filters) -> Self:
        # todo: применить filtering
        obj = self._clone()
        for field, value in filters.items():
            model = obj._model
            column, op = None, operators.eq
            joins = obj._joins
            for attr in field.split("__"):
                if mapped := getattr(model, attr, None):
                    relationships = inspect(model).relationships
                    if attr in relationships:
                        relationship = relationships[attr].class_attribute
                        joins = joins.setdefault(relationship, {})
                        model = relationships[attr].mapper.class_
                    column = mapped
                else:
                    op = obj._operators[attr]
            obj._where.add(op(column, value))
        return obj

    def options(self, *fields: str) -> Self:
        # todo: обработать отсутствие связи
        obj = self._clone()
        for field in fields:
            model = obj._model
            options = obj._options
            joins = obj._joins
            for attr in field.split("__"):
                relationships = inspect(model).relationships
                relationship = relationships[attr].class_attribute
                options = options.setdefault(relationship, {})
                joins = joins.setdefault(relationship, {})
                model = relationships[attr].mapper.class_
        return obj

    def order_by(self, *fields: str) -> Self:
        # todo: обработать одни и повторяющиеся поля
        obj = self._clone()
        for field in fields:
            model = obj._model
            column = None
            joins = obj._joins
            for attr in field.lstrip("-+").split("__"):
                relationships = inspect(model).relationships
                if attr in relationships:
                    relationship = relationships[attr].class_attribute
                    joins = joins.setdefault(relationship, {})
                    model = relationships[attr].mapper.class_
                else:
                    column = getattr(model, attr)
            column = column.desc() if field.startswith("-") else column.asc()
            obj._ordering_fields.add(column)
        return obj

    def _apply_options(self, stmt: Select) -> Select:
        obj = self._clone()
        for relations in obj._flat_options(obj._options):
            joins = obj._joins
            option = None
            for relation in relations:
                if relation in joins:
                    option = (
                        option.contains_eager(relation)
                        if option
                        else contains_eager(relation)
                    )
                else:
                    option = (
                        option.joinedload(relation) if option else joinedload(relation)
                    )
                joins = joins.get(relation, {})
            stmt = stmt.options(option)
        return stmt

    def _flat_options(self, options, prefix=None):
        if prefix is None:
            prefix = []
        result = []
        for key, value in options.items():
            new_prefix = prefix + [key]
            if isinstance(value, dict) and not value:
                result.append(new_prefix)
            elif isinstance(value, dict):
                result.extend(self._flat_options(value, new_prefix))
        return result

    async def all(self):
        result = await self._session.scalars(self.query)
        return result.unique().all()

    async def first(self):
        obj = self._clone()
        obj._limit = 1
        return await self._session.scalar(obj.query)

    def _apply_where(self, stmt: Select) -> Select:
        return stmt.where(*self._where)

    def _apply_order(self, stmt: Select) -> Select:
        return stmt.order_by(*list(self._ordering_fields))

    def _apply_joins(self, stmt: Select, joins: dict) -> Select:
        joins = deepcopy(joins)
        for join, value in joins.items():
            isouter = value.pop("isouter", False)
            stmt = stmt.join(join, isouter=isouter)
            stmt = self._apply_joins(stmt, value)
        return stmt

    def outerjoin(self, *joins) -> Self:
        return self._join(joins, isouter=True)

    def innerjoin(self, *joins) -> Self:
        return self._join(joins, isouter=False)

    def _join(self, joins, isouter) -> Self:
        obj = self._clone()
        for join in joins:
            model = obj._model
            nn_joins = obj._joins
            prev, last = None, None
            for attr in join.split("__"):
                relationships = inspect(model).relationships
                relationship = relationships[attr]
                prev = nn_joins
                kl_attr = last = relationship.class_attribute
                nn_joins = nn_joins.setdefault(kl_attr, {})
                model = relationship.mapper.class_
            prev[last]["isouter"] = isouter
        return obj

    def limit(self, limit: int) -> Self:
        obj = self._clone()
        obj._limit = limit
        return obj

    def offset(self, offset: int) -> Self:
        obj = self._clone()
        obj._offset = offset
        return obj

    @property
    def query(self):
        # todo: запросы delete, update
        stmt = self._apply_joins(self._stmt, self._joins)
        stmt = self._apply_options(stmt)
        stmt = self._apply_where(stmt)
        stmt = self._apply_order(stmt)
        if self._limit or self._offset:
            subquery = select(self._model.id)
            subquery = self._apply_joins(subquery, self._joins)
            subquery = self._apply_where(subquery)
            # сортировка не нужна
            if self._limit is not None:
                subquery = subquery.limit(self._limit)
            if self._offset is not None:
                subquery = subquery.offset(self._offset)
            stmt = stmt.where(self._model.id.in_(subquery))
        return stmt

    # todo: другие методы
    #
    # async def last(self):
    #     pass
    #
    # async def latest(self):
    #     pass
    #
    # async def earliest(self):
    #     pass
    #
    # async def update(self):
    #     pass
    #
    # async def delete(self):
    #     pass
    #
    # ...

```

На что можно обратить внимание, глядя на класс QuerySet.

### lookup-ы

Первое, что бросается в глаза - словарь `_operators`. Он содержит `lookup`-ы и соответствующие им методы SQLAlchemy.

Затем можно видеть публичные и непубличные методы. 

### Промежуточные методы

Это методы пошагового формирования финального запроса:`filter()`, `order_by()`, `options()`, `outerjoin()`, `innerjoin()`, 
`limit()`, `offset()`, которые не выполняют запросов в базу данных. Каждый из этих методов возвращает копию `QuerySet`.

### Терминальные методы

Эти методы выполняют запросы в базу данных: `all()`, `first()` (другие методы в разработке).

### Финальный сформированный запрос

Посмотреть, каким получается итоговый SQL-запрос можно в property `QuerySet.query`.

## Формирование запроса со связными моделями данных

ВСЕ СВЯЗНЫЕ МОДЕЛИ JOIN-ЯТСЯ ДРУГ К ДРУГУ. 

Из-за этого могут быть проблемы с производительностью запросов. Такой стиль был выбран ради возможности фильтровать 
обратные связи.

## Примеры

### Простая фильтрация

Для кода:

```python
repository = SectionRepository(session)
queryset = (
    repository
    .objects
    .filter(name__icontains='управление')
)
await queryset.all()
```

будет сформирован SQL-запрос:

```sql
SELECT sections.id, sections.name, sections.status_id 
FROM sections 
WHERE sections.name ILIKE '%управление%'
```

### Фильтрация по связной модели

Для кода:

```python
repository = SectionRepository(session)
queryset = (
    repository
    .objects
    .filter(status__code='unpublished')
)
await queryset.all()
```

будет сформирован запрос: 

```sql
SELECT sections.id, sections.name, sections.status_id 
FROM sections JOIN publication_statuses ON publication_statuses.id = sections.status_id 
WHERE publication_statuses.code = 'unpublished'
```

Обратите внимание, что автоматически была при-join-ена таблица `publication_statuses`.

### Простая сортировка

Для кода:

```python
repository = SectionRepository(session)
queryset = (
    repository
    .objects
    .order_by('name', '-status_id')
)
await queryset.all()
```

будет сформирован запрос: 

```sql
SELECT sections.id, sections.name, sections.status_id 
FROM sections 
ORDER BY sections.name ASC, sections.status_id DESC
```

Направление сортировки учтено.

### Сортировка по полю связной модели

Для кода:

```python
repository = SectionRepository(session)
queryset = (
    repository
    .objects
    .order_by('status__code')
)
await await queryset.all()
```

```sql
SELECT sections.id, sections.name, sections.status_id 
FROM sections 
JOIN publication_statuses ON publication_statuses.id = sections.status_id 
ORDER BY publication_statuses.code ASC
```

Обратите внимание, что автоматически была при-join-ена таблица `publication_statuses`.

### options

`options` используется для того, что подтянуть в поля relationship значения связных моделей.

Для работы с `options` реализован метод `QuerySet.options()`. Как было написано ранее, связные модели, 
вне зависимости от того, прямые это связи или обратные, они join-ятся. 

Например, для кода:

```python
repository = SectionRepository(session)
queryset = (
    repository
    .objects
    .options('subsections')
)
await queryset.all()
```

где `subsections` - обратная связь на модель Subsection, будет сформирован запрос:

```sql
SELECT subsections.id, subsections.name, subsections.section_id, subsections.status_id, sections.id AS id_1, sections.name AS name_1, sections.status_id AS status_id_1 
FROM sections 
JOIN subsections ON sections.id = subsections.section_id
```

Обратите внимание, чтобы был использован inner join. Соответственно, для примера будут возвращены только те Section, 
у которых есть связные Subsection (joinedload по умолчанию используется inner join). Данные полученные запросом будут примерно 
следующими:

```json
[
  {
    "status_id": 2,
    "name": "Управление аккаунтом",
    "id": 1,
    "subsections": [
      {
        "id": 1,
        "status_id": 2,
        "name": "Полезные документы",
        "section_id": 1
      }
    ]
  }
]
```

Но что делать, если необходимо получить все Section, даже если у них отсутствуют связные Subsection?

Для этого необходимо вручную задать тип join-а, чтобы QuerySet подтянул связные записи при помощи contains_eager:

```python
repository = SectionRepository(session)
queryset = (
    repository
    .objects
    .outerjoin('subsections')
    .options('subsections')
)
await queryset.all()
```

Тогда будет использован outer join:

```sql
SELECT subsections.id, subsections.name, subsections.section_id, subsections.status_id, sections.id AS id_1, sections.name AS name_1, sections.status_id AS status_id_1 
FROM sections 
LEFT OUTER JOIN subsections ON sections.id = subsections.section_id
```

А в результате будут все Section:

```json
[
  {
    "status_id": 2,
    "name": "Управление аккаунтом",
    "id": 1,
    "subsections": [
      {
        "id": 1,
        "status_id": 2,
        "name": "Полезные документы",
        "section_id": 1
      }
    ]
  },
  {
    "status_id": 2,
    "name": "Личный кабинет подрядчика ТС5",
    "id": 6,
    "subsections": []
  },
  {
    "status_id": 2,
    "name": "Настройки",
    "id": 7,
    "subsections": []
  },
  {
    "status_id": 1,
    "name": "Управление доступом",
    "id": 2,
    "subsections": []
  },
  {
    "status_id": 2,
    "name": "Финансовые документы Х5 Недвижимость",
    "id": 5,
    "subsections": []
  },
  {
    "status_id": 2,
    "name": "Заявки и консультации",
    "id": 4,
    "subsections": []
  },
  {
    "status_id": 2,
    "name": "Действующие договоры с Х5",
    "id": 3,
    "subsections": []
  }
]
```

### Получение Section, у которых отсутствуют связные Subsection

Выше был приведен один кейс использования метода `QuerySet.outerjoin`.

Еще одним примером может быть кейс, когда необходимо получить только те Section, у которых отсутствуют Subsection. 
Для этого также определяем `outerjoin` и фильтруем по условию `Subsection.id = null`:

```python
repository = SectionRepository(session)
queryset = (
    repository
    .objects
    .outerjoin('subsections')
    .filter(subsections__section_id=None)
    .options('subsections')
)
await queryset.all()
```

Код сгенерирует запрос:

```sql
SELECT subsections.id, subsections.name, subsections.section_id, subsections.status_id, sections.id AS id_1, sections.name AS name_1, sections.status_id AS status_id_1 
FROM sections 
LEFT OUTER JOIN subsections ON sections.id = subsections.section_id 
WHERE subsections.section_id IS NULL
```

Результат:

```json
[
  {
    "status_id": 1,
    "name": "Управление доступом",
    "id": 2,
    "subsections": []
  },
  {
    "status_id": 2,
    "name": "Действующие договоры с Х5",
    "id": 3,
    "subsections": []
  },
  {
    "status_id": 2,
    "name": "Заявки и консультации",
    "id": 4,
    "subsections": []
  },
  {
    "status_id": 2,
    "name": "Финансовые документы Х5 Недвижимость",
    "id": 5,
    "subsections": []
  },
  {
    "status_id": 2,
    "name": "Личный кабинет подрядчика ТС5",
    "id": 6,
    "subsections": []
  },
  {
    "status_id": 2,
    "name": "Настройки",
    "id": 7,
    "subsections": []
  }
]
```

### Получение первой записи

Для получения первой (любой) записи разработан метод `QuerySet.first()`. Его использование связано с применением метод 
`limit` SQLAlchemy. Это в свою очередь при использовании только join-а связных моделей приводит к некоторым побочным 
эффектам. Можно догадаться, что при join-е обратных связей в результирующую выборку попадет только одна запись обратной
связи. Поэтому пришлось сделать то, что можно увидеть в property `QuerySet.query`, а именно подзапрос.

Так, код:

```python
repository = SectionRepository(session)
queryset = (
    repository
    .objects
    .options('subsections')
)
await queryset.first()
```

сгенерирует запрос:

```sql
SELECT subsections.id,
       subsections.name,
       subsections.section_id,
       subsections.status_id,
       sections.id AS id_1,
       sections.name AS name_1,
       sections.status_id AS status_id_1
FROM sections
JOIN subsections ON sections.id = subsections.section_id
WHERE sections.id IN
    (SELECT sections.id
     FROM sections
     JOIN subsections ON sections.id = subsections.section_id
     LIMIT $1::INTEGER)
```

При множественных запрашиваемых связях, возможно, будет проседать прозводительность запросов.

### Кастомный objects

Возможно создать собственный objects с предустановленными фильтрами, сортировками и тд. 

Например, если приходится часто работать только с Section со статусом published. Тогда в репозиторий 
добавляется метод `published`:

```python
class SectionRepository(BaseRepository):
    model = Section

    @property
    def published(self):
        return self.objects.filter(status__code='published')
```

Тогда код:

```python
repository = SectionRepository(session)
queryset = (
    repository
    .published
)
await queryset.all()
```

сгенерирует SQL-запрос:

```sql
SELECT sections.id, sections.name, sections.status_id 
FROM sections 
JOIN publication_statuses ON publication_statuses.id = sections.status_id 
WHERE publication_statuses.code = 'published'
```

## Планы

- реализовать больше методов 
- покрыть тестами
- проверить в полевых условиях