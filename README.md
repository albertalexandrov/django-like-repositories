# django-like-repositories

# Оглавление

- [Модели данных](#модели-данных)
- [Базовый репозиторий](#базовый-репозиторий)
- [QuerySet](#queryset)
  - [Промежуточные и терминальные методы](#промежуточные-и-терминальные-методы)
  - [Вычисление QuerySet](#вычисление-queryset)
  - [Срезы](#срезы)
  - [Управление жизенным циклом SQLAlchemy](#управление-жизенным-циклом-sqlalchemy)
  - [Кэширование](#кэширование)
- [QueySet API](#queryset-api)
  - [filter()](#filter)
  - [order_by()](#order_by)
  - [options()](#options)
  - [innerjoin()](#innerjoin)
  - [outerjoin()](#outerjoin)
  - [execution_options()](#execution_options)
  - [returning()](#returning)
  - [flush()](#flush)
  - [commit()](#commit)
  - [values_list()](#values_list)
  - [distinct()](#distinct)
  - [first()](#first)
  - [count()](#count)
  - [get_one_or_none()](#get_one_or_none)
  - [get_or_create()](#get_or_create)
  - [update_or_create()](#update_or_create)
  - [in_bulk()](#in_bulk)
  - [exists()](#exists)
  - [delete()](#delete)
- [QueryBuilder](#querybuilder)
- [API QueryBuilder](#api-querybuilder)
  - [filter()](#filter-1)
  - [order_by()](#order_by-1)
  - [options()](#options-1)
  - [returning()](#returning-1)
  - [execution_options()](#execution_options-1)
  - [values_list()](#values_list-1)
  - [join()](#join)
  - [distinct()](#distinct-1)
  - [limit()](#limit)
  - [offset()](#offset)
  - [build_count_stmt()](#build_count_stmt)
  - [build_delete_stmt()](#build_delete_stmt)
  - [build_update_stmt()](#build_update_stmt)
  - [build_select_stmt()](#build_select_stmt)
- [Примеры](#примеры)
  - [Простая фильтрация](#простая-фильтрация)
  - [Фильтрация по связной модели](#фильтрация-по-связной-модели)
  - [Простая сортировка](#простая-сортировка)
  - [Сортировка по полю связной модели](#сортировка-по-полю-связной-модели)
  - [options](#options)
  - [Получение Section, у которых отсутствуют связные Subsection](#получение-section-у-которых-отсутствуют-связные-subsection)
  - [Получение первой записи](#получение-первой-записи)
  - [Кастомный objects](#кастомный-objects)
  - [Лимитированные запросы](#лимитированные-запросы)
  - [Обновление данных](#обновление-данных)
  - [Удаление данных](#удаление-данных)
- [Планы](#планы)

## Дисклеймер

Весь код mvp-ишный.

## Идеи, замечания

Приветствуются

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
    model_cls = Section
```

## Базовый репозиторий

Плюс-минус стандартный:

```python
from itertools import islice
from typing import Generic, Any, Type, Self

from fastapi.params import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from dependencies import get_session
from repositories.queryset import QuerySet
from repositories.types import Model


class BaseRepository(Generic[Model]):
    model_cls: Type[Model] = None

    def __init__(self, session: AsyncSession = Depends(get_session)):
        if not self.model_cls:
            raise ValueError("Не задана модель в атрибуте `model_cls`")
        self._session = session
        self._flush = None
        self._commit = None

    def _clone(self) -> Self:
        clone = self.__class__(session=self._session)
        clone._flush = self._flush
        clone._commit = self._commit
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
        self._flush = None
        self._commit = None

    async def create(self, **kw: dict[str:Any]) -> Model:
        obj = self.model_cls(**kw)
        self._session.add(obj)
        await self._flush_commit_reset(obj)
        return obj

    async def bulk_create(self, values: list[dict], batch_size: int = None) -> list[Model]:
        if batch_size is not None and (not isinstance(batch_size, int) or batch_size <= 0):
            raise ValueError("batch_size должен быть целым положительным числом")
        objs = []
        if batch_size:
            it = iter(values)
            while batch := list(islice(it, batch_size)):
                batch_objs = [self.model_cls(**item) for item in batch]
                await self._flush_commit_reset(*batch_objs)
                objs.extend(batch_objs)
        else:
            for item in values:
                obj = self.model_cls(**item)
                objs.append(obj)
            await self._flush_commit_reset(objs)
        return objs

    async def get_by_pk(self, pk: Any) -> Model:
        return await self._session.get(self.model_cls, pk)

    @property
    def objects(self) -> QuerySet:
        return QuerySet(self.model_cls, self._session)

```

Рассмотрим класс `QuerySet`.

## QuerySet

Поделка на QuerySet Django с некоторыми особенностями SQLAlchemy.

Данный класс принимает параметры запроса при помощи промежуточныех методов и транслирует их в QueryBuilder, 
а также выполняет запросы в БД.

### Промежуточные и терминальные методы

Класс содержит методы, которые деляться на два типа:
- промежуточные и
- терминальные.

Промежуточные методы - `filter()`, `order_by()`, `returning()`, `innerjoin()`, `outerjoin()`, `options()`,
`execution_options()`, `values_list()`, `distinct()`, `flush()`, `commit()`) - не выполняют запросов в БД, а
предназначены для того, чтобы принимать параметры запроса (параметры фильтрации, сортировки и тд)
Промежуточные методы возвращают копию QuerySet.

Терминальные методы - `first()`, `count()`, `get_one_or_none()`, `delete()`, `update()`, `exists()`, `in_bulk()`,
`update_or_create()`, `get_or_create()` - соответственно, выполняют запросы в БД.

### Вычисление QuerySet

Вычисляется QuerySet простым await-ом:

```python
qs = some_repository.object.filter(status_code="published")
result = await qs
```

### Срезы

Лимитировать QuerySet можно при помощи срезов (шаг среза не поддерживается).  Для этого необходимо
передать срез:

```python
qs = some_repository.object.filter(status_code="published")[10:20]
result = await qs
```

Это добавит в итоговый запрос `LIMIT` и `OFFSET`. Также возможно задать индекс:

```python
qs = some_repository.object.filter(status_code="published")[0]
obj = await qs
```

И тогда это вернет объект, а не список

### Управление жизенным циклом SQLAlchemy

Иногда необходимо выполнить flush или commit после выполнения запроса или, напр., для получения id
вновь созданного объекта (для этого выполняется flush).  Для этого необходимо дать инструкции при
помощих соответствующих методов `flush()` и `commit()`:

```python
await some_repository.object.filter(status_code="published").commit().delete()
```

Параметры управления жизненным циклом сессии определяются для каждого запроса

### Кэширование

Результат вычисления QuerySet не кэшируется.

## QuerySet API

### filter()

#### filter(**kw: dict[str:Any]) -> Self

Передает параметры фильтрации в QueryBuilder.

Промежуточный метод. 

Возвращает копию QuerySet.

### order_by()

#### order_by(*args: str) -> Self

Передает параметры сортировки в QueryBuilder.

Промежуточный метод. 

Возвращает копию QuerySet.

### options

#### options(*args: str) -> Self

Передает параметры options в QueryBuilder

### innerjoin()

#### innerjoin(*args: str) -> Self

Передает параметры внутреннених join-ов в QueryBuilder.

Промежуточный метод. 

Возвращает копию QuerySet.

### outerjoin()

#### outerjoin(*args: str) -> Self

Передает параметры внешних join-ов в QueryBuilder.

Промежуточный метод. 

Возвращает копию QuerySet.

### execution_options()

#### execution_options(**kw: dict[str:Any]) -> Self

Передает параметры выполнения запроса в QueryBuilder.

Промежуточный метод. 

Возвращает копию QuerySet.

### returning()

#### returning(*args: str, return_model: bool = False) -> Self

Передает параметры возвращаемых значений в QueryBuilder.

Промежуточный метод. 

Возвращает копию QuerySet.

### flush()

#### flush(flush: bool = True, /) -> Self

Сохраняет указание на выполнение flush после выполнения запроса

Промежуточный метод. 

Возвращает копию QuerySet.

### commit()

#### commit(commit: bool = True, /) -> Self

Сохраняет указание на выполнение commit после выполнения запроса. 

Промежуточный метод. 

Возвращает копию QuerySet.

### values_list()

#### values_list(*args: str, flat: bool = False, named: bool = False) -> Self

Передает названия запрашиваемых столбцов в QueryBuilder

Промежуточный метод. 

Возвращает копию QuerySet.

### distinct()

#### distinct() -> Self

Передает указание применить DISTINCT в QueryBuilder.

Промежуточный метод. 

Возвращает копию QuerySet.

### first()

#### first() -> Model | None

Возвращает первый элемент QuerySet.

Терминальный метод.

### count()

#### count() -> int

Возвращает количество объектов в QuerySet.

Терминальный метод.

### get_one_or_none()

#### get_one_or_none() -> Model | None

Возвращает первый объект в QuerySet или None. Если элементов больше одного, то рейзится исключение.

Терминальный метод.

### get_or_create()

#### get_or_create(defaults: dict = None, **kw) -> tuple[Model, bool]

Возвращает объект или создает новый, если объект по условиям не был найден.

Терминальный метод.

### update_or_create()

#### update_or_create(defaults=None, create_defaults=None, **kw) -> tuple[Model, bool]

Обновляет сущетсвующий объект или создает новый, если объект по условиям не найден.

Терминальный метод.

### in_bulk()

#### in_bulk(id_list: list[Any] = None, *, field_name="id") -> dict[Any:Model]

Возвращает словарь, где в качестве ключа выступает значение из field_name, а значением - объект.

Терминальный метод.

### exists()

#### exists() -> bool

Возвращает признак наличия объектов в QuerySet.

Терминальный метод.

### delete()

#### delete() -> Result[Model]

Выполняет удаление объектов, входящих в QuerySet.

Терминальный метод.

### update()

#### update(values: dict[str:Any]) -> Result[Model]

Выполняет обновление объектов, входящих в QuerySet.

Терминальный метод.

## QueryBuilder

Обертка над запросом SQLAlchemy.  Хранит параметры запроса.  Предоставляет методы для создания конечных методов.
Собирает параметры запроса и в конце генерирует запрос.

**ВАЖНО! Все связные модели JOIN-ятся. Такой подход был выбран по нескольким причинам: 

- относительная простота разработки, особенно в контексте работы с обратными связями и кейсов типа "вернуть только
те разделы, у которых есть подразделы" (или наоборот);
- относительно проще воспринимать и контролировать построение запроса (ведь запрос в итоге всего один).**

## API QueryBuilder

### filter()

#### filter(**kw: dict[str:Any]) -> None

Парсит и валидирует условия фильтрации, обрабатывает сопутствующие join-ы.

### order_by()

#### order_by(*args: str) -> None

Парсит и валидирует условия сортировки, обрабатывает сопутствующие join-ы.

### options()

#### options(*args: str) -> None

Парсит и валидирует options, обрабатывает сопутствующие join-ы.

Найденные JOIN-ы сохраняются вместе в JOIN-ами, найденными при парсинге условий фильтрации и сортировки.

### returning()

#### returning(*args: str, return_model: bool = False) -> None

Парсит и валидирует возвращаемые значения.

### execution_options()

#### execution_options(**kw: dict[str, Any]) -> None

Сохраняет условия выполнения запроса.

### values_list()

#### values_list(*args: str) -> None:

Парсит и валидирует наименования возвращаемых столбцов.

### join()

#### join(*args: str, isouter: bool) -> None

Парсит и валидирует JOIN-ы

### distinct()

#### distinct() -> None

Сохраняет указание применить DISTINCT

### limit()

#### limit(self, limit: int | None) -> None

Сохраняет значение для LIMIT.

### offset()

#### offset(self, offset: int | None) -> None

Сохраняет значение для OFFSET.

### build_count_stmt()

#### build_count_stmt() -> Select

Возвращает запрос на подсчет количества.

### build_delete_stmt()

#### build_delete_stmt(self) -> Delete

Возвращает запрос на удаление.

### build_update_stmt()

#### build_update_stmt(self, values: dict[str, Any]) -> Update

Возвращает запрос на обновление.

### build_select_stmt()

#### build_select_stmt(self) -> Select

Возаращает запрос на выборку данных.

## Примеры

### Простая фильтрация

Для кода:

```python
repository = SectionRepository(session)
qs = (
    repository
    .objects
    .filter(name__icontains='управление')
)
res = await qs
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
qs = (
    repository
    .objects
    .filter(status__code='unpublished')
)
sections = await qs
```

будет сформирован запрос: 

```sql
SELECT sections.id, sections.name, sections.status_id 
FROM sections 
JOIN publication_statuses AS publication_statuses_1 ON publication_statuses_1.id = sections.status_id 
WHERE publication_statuses_1.code = 'unpublished'
```

Обратите внимание, что автоматически была при-JOIN-ена таблица `publication_statuses`. JOIN произошел через алиас.
Также фильтрация производится через алиас.

Таблицы JOIN-ятся через алиасы для того, чтобы иметь возможность JOIN-ить одинаковые таблицы более чем один раз и 
фильтроваться по ним.

### Простая сортировка

Для кода:

```python
repository = SectionRepository(session)
qs = (
    repository
    .objects
    .order_by('name', '-status_id')
)
sections = await qs
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
qs = (
    repository
    .objects
    .order_by('status__code')
)
sections = await qs
```

```sql
sections.id, sections.name, sections.status_id 
FROM sections 
JOIN publication_statuses AS publication_statuses_1 ON publication_statuses_1.id = sections.status_id 
ORDER BY publication_statuses_1.code ASC
```

Обратите внимание, что автоматически была при-join-ена таблица `publication_statuses`.

### options

`options` используется для того, что подтянуть в поля relationship значения связных моделей.

Для работы с `options` реализован метод `QuerySet.options()`. Как было написано ранее, связные модели, 
вне зависимости от того, прямые это связи или обратные, они join-ятся. 

Например, для кода:

```python
repository = SectionRepository(session)
qs = (
    repository
    .objects
    .options('subsections')
)
sections = await qs
```

где `subsections` - обратная связь на модель Subsection, будет сформирован запрос:

```sql
SELECT subsections_1.id, subsections_1.name, subsections_1.section_id, subsections_1.status_id, sections.id AS id_1, sections.name AS name_1, sections.status_id AS status_id_1 
FROM sections 
JOIN subsections AS subsections_1 ON sections.id = subsections_1.section_id
```

Обратите внимание, чтобы был использован INNER JOIN. Соответственно, для примера будут возвращены только те Section, 
у которых есть связные Subsection. Данные полученные запросом будут примерно следующими:

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

Для этого необходимо вручную задать тип JOIN-а, чтобы QuerySet подтянул связные записи при помощи contains_eager:

```python
repository = SectionRepository(session)
qs = (
    repository
    .objects
    .outerjoin('subsections')
    .options('subsections')
)
sections = await qs
```

Тогда будет использован LEFT JOIN:

```sql
SELECT subsections_1.id, subsections_1.name, subsections_1.section_id, subsections_1.status_id, sections.id AS id_1, sections.name AS name_1, sections.status_id AS status_id_1 
FROM sections 
LEFT OUTER JOIN subsections AS subsections_1 ON sections.id = subsections_1.section_id
```

А в результате будут все Section в том числе те, у которых нет Subsection:

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

Выше был приведен один кейс использования метода `QuerySet.outerjoin()`.

Еще одним примером может быть кейс, когда необходимо получить только те Section, у которых отсутствуют Subsection. 
Для этого также определяем `outerjoin` и фильтруем по условию `Subsection.id = null`:

```python
repository = SectionRepository(session)
qs = (
    repository
    .objects
    .outerjoin('subsections')
    .filter(subsections__section_id=None)
    .options('subsections')
)
sections = await qs
```

Код сгенерирует запрос:

```sql
SELECT subsections_1.id, subsections_1.name, subsections_1.section_id, subsections_1.status_id, sections.id AS id_1, sections.name AS name_1, sections.status_id AS status_id_1 
FROM sections 
LEFT OUTER JOIN subsections AS subsections_1 ON sections.id = subsections_1.section_id 
WHERE subsections_1.section_id IS NULL
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
qs = (
    repository
    .objects
    .options('subsections')
)
section = await qs.first()
```

сгенерирует запрос:

```sql
SELECT subsections_1.id, subsections_1.name, subsections_1.section_id, subsections_1.status_id, anon_1.id AS id_1, anon_1.name AS name_1, anon_1.status_id AS status_id_1 
FROM (
    SELECT DISTINCT sections.id AS id, sections.name AS name, sections.status_id AS status_id 
    FROM sections 
    JOIN subsections AS subsections_2 ON sections.id = subsections_2.section_id 
    LIMIT 1 OFFSET 0
) AS anon_1 
JOIN subsections AS subsections_1 ON anon_1.id = subsections_1.section_id
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
qs = (
    repository
    .published
)
sections = await qs
```

сгенерирует SQL-запрос:

```sql
SELECT sections.id, sections.name, sections.status_id 
FROM sections 
JOIN publication_statuses AS publication_statuses_1 ON publication_statuses_1.id = sections.status_id 
WHERE publication_statuses_1.code = 'published'
```

### Лимитированные запросы

Лимитированные запросы выполняются при помощи срезов:

```python
repository = SectionRepository(session)
qs = (
    repository
    .objects
    .options('subsections')
)
sections = await qs[:1]
```

сгенерирует SQL-запрос:

```sql
SELECT subsections_1.id, subsections_1.name, subsections_1.section_id, subsections_1.status_id, anon_1.id AS id_1, anon_1.name AS name_1, anon_1.status_id AS status_id_1 
FROM (
    SELECT DISTINCT sections.id AS id, sections.name AS name, sections.status_id AS status_id 
    FROM sections JOIN subsections AS subsections_2 ON sections.id = subsections_2.section_id 
    LIMIT 1 
    OFFSET 0
) AS anon_1 
JOIN subsections AS subsections_1 ON anon_1.id = subsections_1.section_id
```

Обратите внимание, что лимитирован подзапрос и в конечном результате все обратные связи войдут в выборку без потерь.

### Обновление данных

Для обновления данных необходимо использовать метод `update()`:

```python
repository = SectionRepository(session)
qs = (
    repository
    .objects
    .filter(status_id=3)
)
await qs.update(status_id=1)
```

Будет сгенерирован SQL-запрос:

```sql
UPDATE sections 
SET status_id=1 
WHERE sections.id IN (
    SELECT distinct(sections.id) AS distinct_1 
    FROM sections 
    WHERE sections.status_id = 3
)
RETURNING sections.id
```

RETURNING можно переопределить методом `returning()`:

```python
qs = (
    repository
    .objects
    .filter(name='раздел15')
    .returning(return_model=True)
)
result = await qs.update(status_id=3)
print(result.scalars().all())
```

где result - это объект Result SQLAlchemy со всеми вытекающими возможностями манипулирования этим объектом.

### Удаление данных

Для удаления данных необходимо использовать метод `delete()`:

```python
repository = SectionRepository(session)
qs = (
    repository
    .objects
    .filter(name='раздел16')
)
await qs.delete()
```

Будет сгенерирован SQL-запрос:

```sql
DELETE FROM sections 
WHERE sections.id IN (
    SELECT distinct(sections.id) AS distinct_1 
    FROM sections 
    WHERE sections.name = 'раздел16'
) 
RETURNING sections.id
```

RETURNING переопределяется также как и в случае с методом `update()`.

## Планы

- реализовать больше методов 
- покрыть тестами
- проверить в полевых условиях