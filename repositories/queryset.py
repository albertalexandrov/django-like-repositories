import operator
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

    def filter(self, *, filtering: Filter = None, **filters) -> Self:
        # apply the given filters to self._stmt
        # filters example:
        #   first_name__in=["Alex", "John"]
        #   type__code="sh" - through relation
        #   etc
        for field, value in filters.items():
            model = self._model
            column, op = None, operators.eq
            joins = self._joins
            for attr in field.split("__"):
                if mapped := getattr(model, attr, None):
                    relationships = inspect(model).relationships
                    if attr in relationships:
                        relationship = relationships[attr].class_attribute
                        joins = joins.setdefault(relationship, {})
                        model = relationships[attr].mapper.class_
                    column = mapped
                else:
                    op = self._operators[attr]
            self._where.add(op(column, value))
        return self

    def options(self, *args: str) -> Self:
        # todo: обработать отсутствие связи
        for arg in args:
            model = self._model
            options = self._options
            for attr in arg.split("__"):
                relationships = inspect(model).relationships
                relationship = relationships[attr].class_attribute
                options = options.setdefault(relationship, {})
                model = relationships[attr].mapper.class_
        return self

    def order_by(self, *fields: str) -> Self:
        # todo: обработать одни и повторяющиеся поля
        for field in fields:
            model = self._model
            column = None
            joins = self._joins
            for attr in field.lstrip("-+").split("__"):
                relationships = inspect(model).relationships
                if attr in relationships:
                    relationship = relationships[attr].class_attribute
                    joins = joins.setdefault(relationship, {})
                    model = relationships[attr].mapper.class_
                else:
                    column = getattr(model, attr)
            column = column.desc() if field.startswith("-") else column.asc()
            self._ordering_fields.add(column)
        return self

    def _apply_options(self, stmt: Select, options: dict, joins: dict, parent=None) -> Select:
        options = deepcopy(options)
        for option, value in options.items():
            if option in joins:
                parent = parent.contains_eager(option) if parent else contains_eager(option)
            else:
                parent = parent.joinedload(option) if parent else joinedload(option)
            stmt = self._apply_options(stmt, value, joins.get(option, {}), parent)
            stmt = stmt.options(parent)
        return stmt

    async def all(self):
        result = await self._session.scalars(self.query)
        return result.all()

    async def first(self):
        stmt = self._stmt.limit(1)
        return await self._session.scalar(stmt)

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
        for join in joins:
            model = self._model
            nn_joins = self._joins
            prev, last = None, None
            for attr in join.split("__"):
                relationships = inspect(model).relationships
                relationship = relationships[attr]
                prev = nn_joins
                kl_attr = last = relationship.class_attribute
                nn_joins = nn_joins.setdefault(kl_attr, {})
                model = relationship.mapper.class_
            prev[last]["isouter"] = isouter
        return self

    @property
    def query(self):
        # {'first_name': 'Иван', 'last_name': 'Иванов'}
        # class M(Base):
        #     day: Mapped[date]
        # m__day__day, m__day
        # type__code, type__status__name__ilike
        stmt = self._apply_joins(self._stmt, self._joins)
        stmt = self._apply_where(stmt)
        stmt = self._apply_order(stmt)
        stmt = self._apply_options(stmt, self._options, self._joins)
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
