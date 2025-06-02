from copy import deepcopy
from typing import Self

from fastapi_filter.contrib.sqlalchemy import Filter
from sqlalchemy import select, extract, inspect, Select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, contains_eager
from sqlalchemy.sql import operators

from models import User


class QuerySet:
    """
    Wrapper for SQLAlchemy session for frequently used cases like filtering with Django like lookups
    Implements builder pattern to build SQLAlchemy statement step by step
    Made for simplified use to fields na d relations
    """
    _operators = {
        'in': operators.in_op,
        'isnull': lambda c, v: (c == None) if v else (c != None),
        'exact': operators.eq,
        'eq': operators.eq,
        'ne': operators.ne,
        'gt': operators.gt,
        'ge': operators.ge,
        'lt': operators.lt,
        'le': operators.le,
        'notin': operators.notin_op,
        'between': lambda c, v: c.between(v[0], v[1]),
        'like': operators.like_op,
        'ilike': operators.ilike_op,
        'startswith': operators.startswith_op,
        'istartswith': lambda c, v: c.ilike(v + '%'),
        'endswith': operators.endswith_op,
        'iendswith': lambda c, v: c.ilike('%' + v),
        'contains': lambda c, v: c.like(f'%{v}%'),
        'icontains': lambda c, v: c.ilike(f'%{v}%'),
        'year': lambda c, v: extract('year', c) == v,
        'year_ne': lambda c, v: extract('year', c) != v,
        'year_gt': lambda c, v: extract('year', c) > v,
        'year_ge': lambda c, v: extract('year', c) >= v,
        'year_lt': lambda c, v: extract('year', c) < v,
        'year_le': lambda c, v: extract('year', c) <= v,
        'month': lambda c, v: extract('month', c) == v,
        'month_ne': lambda c, v: extract('month', c) != v,
        'month_gt': lambda c, v: extract('month', c) > v,
        'month_ge': lambda c, v: extract('month', c) >= v,
        'month_lt': lambda c, v: extract('month', c) < v,
        'month_le': lambda c, v: extract('month', c) <= v,
        'day': lambda c, v: extract('day', c) == v,
        'day_ne': lambda c, v: extract('day', c) != v,
        'day_gt': lambda c, v: extract('day', c) > v,
        'day_ge': lambda c, v: extract('day', c) >= v,
        'day_lt': lambda c, v: extract('day', c) < v,
        'day_le': lambda c, v: extract('day', c) <= v,
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

    def _apply_options(self, stmt: Select, options: dict, joins: dict, parent=None) -> Select:
        obj = self._clone()
        options = deepcopy(options)
        for option, value in options.items():
            print(option)
            if option in joins:
                parent = parent.contains_eager(option) if parent else contains_eager(option)
            else:
                parent = parent.joinedload(option) if parent else joinedload(option)
            stmt = obj._apply_options(stmt, value, joins.get(option, {}), parent)
            stmt = stmt.options(parent)
        return stmt

    async def all(self):
        result = await self._session.scalars(self.query)
        return result.all()

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
        # todo:
        #  рассмотреть кейс, когда название поля модели совпадает в lookup-ом
        #  class M(Base):
        #     day: Mapped[date]
        #  m__day__day, m__day
        stmt = self._apply_joins(self._stmt, self._joins)
        stmt = self._apply_options(stmt, self._options, self._joins)
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
