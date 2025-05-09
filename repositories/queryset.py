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
        self._joins = {}
        self._options = []

    def filter(self, **filters):
        for attr, value in filters.items():
            model = self._model
            relationships = inspect(self._model).relationships
            if "__" in attr:
                parts = attr.split("__")
                if parts[-1] in self._operators:
                    op = self._operators.pop(parts[-1])
                else:
                    op = operators.eq

                attr_name = None
                iii = self._joins
                for i in parts:
                    if i in relationships:
                        iii = iii.setdefault(i, {})
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

    def options(self, *args):
        self._options.extend(args)
        return self

    def order_by(self, *fields):
        for field in fields:
            col = self._extract_column(field.lstrip("-+"))
            col = col.desc() if field.startswith("-") else col.asc()
            self._stmt = self._stmt.order_by(col)
        return self

    def _extract_column(self, field: str):
        # example of field: type__status__code, type__code
        model = self._model
        attr_name = None
        if "__" in field:
            parts = field.split("__")
            joins = self._joins
            relationships = inspect(model).relationships
            for part in parts:
                if part in relationships:
                    joins = joins.setdefault(part, {})
                    model = relationships[part].mapper.class_
                    relationships = inspect(model).relationships
                    self._stmt = self._stmt.join(model)
                else:
                    attr_name = part
        else:
            attr_name = field
        return getattr(model, attr_name)

    def apply_options(self):
        for arg in self._options:
            prom_join = self._joins
            if "__" in arg:
                first, rest = arg.split("__", 1)
                joinit = getattr(self._model, first)
                option = contains_eager(joinit) if first in prom_join else joinedload(joinit)
                prom_join = prom_join.get(first, {})
                relationships = inspect(self._model).relationships
                model = relationships[first].mapper.class_
                for i in rest.split("__"):
                    n = getattr(model, i)
                    if i in prom_join:
                        option = option.contains(n)
                    else:
                        option = option.joinedload(n)
                    relationships = inspect(model).relationships
                    model = relationships[i].mapper.class_
            else:
                joinit = getattr(self._model, arg)
                option = contains_eager(joinit) if arg in prom_join else joinedload(joinit)
            self._stmt = self._stmt.options(option)

    async def all(self):
        self.apply_options()
        result = await self._session.scalars(self._stmt)
        return result.all()

    async def first(self):
        stmt = self._stmt.limit(1)
        return await self._session.scalar(stmt)
