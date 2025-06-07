from copy import deepcopy
from typing import Self

from fastapi_filter.contrib.sqlalchemy import Filter
from sqlalchemy import select, extract, inspect, Select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, contains_eager
from sqlalchemy.sql import operators

from exceptions import ObjectNotFoundError


# todo:
#  по идее подзапрос нужно строить только тогда, когда нужно подгрузить options
#  без них же можно применить distinct


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
        # todo: проверить, что происходит с изменяемыми атрибутами при изменении этих атрибутов в копиях
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
    def _model_pk(self):
        # todo: проверить составные первичные ключи
        return inspect(self._model).primary_key

    def _get_count_stmt(self):
        stmt = select(func.count(func.distinct(*self._model_pk))).select_from(self._model)
        stmt = self._apply_joins(stmt, self._joins)
        stmt = self._apply_where(stmt)
        return stmt

    async def count(self) -> int:
        obj = self._clone()
        stmt = obj._get_count_stmt()
        return await obj._session.scalar(stmt)

    async def get_one_or_raise(self, exc: Exception = ObjectNotFoundError):
        # если делать в Django, то есть давать возможность осуществлять фильтрацию как в методе get,
        # то может возникнуть коллизия в названиях фильтра и exc
        # todo: может два сразу запрашивать?
        obj = self._clone()
        count = await self.count()
        if count == 0:
            raise exc
        elif count > 1:
            raise ValueError  # MultipleObjectsReturned  # todo: поменять значение
        return await obj.first()

    @property
    def query(self):
        # todo:
        #  рассмотреть кейс, когда название поля модели совпадает в lookup-ом
        #  class M(Base):
        #     day: Mapped[date]
        #  m__day__day, m__day
        # todo:
        #  options добавляет в select полей. если options не заданы, то можно не делать подзапрос
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

    # todo: methods
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
