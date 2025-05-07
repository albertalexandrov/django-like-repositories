from sqlalchemy import select, extract, inspect
from django.db.models import QuerySet as DjangoQuerySet
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import relationship, joinedload, contains_eager
from sqlalchemy.orm.base import _entity_descriptor
from sqlalchemy.sql import operators

from models import UserType


class QuerySet:
    _operators = {
        'isnull': lambda c, v: (c == None) if v else (c != None),  # noqa: E711
        'exact': operators.eq,
        'eq': operators.eq,  # equal
        'ne': operators.ne,  # not equal or is not (for None)
        'gt': operators.gt,  # greater than , >
        'ge': operators.ge,  # greater than or equal, >=
        'lt': operators.lt,  # lower than, <
        'le': operators.le,  # lower than or equal, <=
        'in': operators.in_op,
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
        self._joins = []

    def rec(self, attr, value):
        pass

    def filter(self, **filters):
        # t = inspect(UserType).relationships
        for attr, value in filters.items():
            model = self._model
            relationships = inspect(self._model).relationships
            if "__" in attr:
                # type__code
                parts = attr.split("__")
                if parts[-1] in self._operators:
                    op = self._operators.pop(parts[-1])
                else:
                    op = operators.eq
                attr_name = None
                join = ""
                for i in parts:  # ["type", "code"]
                    print("f =>", i)
                    if i in relationships:
                        join += f"__{i}"
                        join = join.lstrip("__")
                        self._joins.append(join)
                        model = relationships[i].mapper.class_
                        relationships = inspect(model).relationships
                        self._stmt = self._stmt.join(model)
                    else:
                        attr_name = i
            else:
                attr_name, op = attr, operators.eq
            column = getattr(model, attr_name)
            self._stmt = self._stmt.where(op(column, value))
        return self

    def select_related(self, *args):
        for arg in args:
            model = self._model
            option = None
            for p in arg.split("__"):
                at = getattr(model, p)
                if p in self._joins:
                    option = option.contains_eager(at) if option else contains_eager(at)
                else:
                    option = option.joinedload(at) if option else joinedload(at)
                relationships = inspect(model).relationships
                model = relationships[p].mapper.class_
            self._stmt = self._stmt.options(option)
        return self

    def order_by(self, *fields):
        for field in fields:
            col = getattr(self._model, field.lstrip("-+"))
            col = col.desc() if field.startswith("-") else col.asc()
            self._stmt = self._stmt.order_by(col)
        return self

    async def all(self):
        result = await self._session.scalars(self._stmt)
        return result.all()

    async def first(self):
        return await self._session.scalar(self._stmt)
