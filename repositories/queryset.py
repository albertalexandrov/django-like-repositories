from fastapi_filter.contrib.sqlalchemy import Filter
from sqlalchemy import select, extract, inspect
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, contains_eager
from sqlalchemy.sql import operators


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
        self._joins = {}
        self._options = []

    def filter(self, *, filtering: Filter = None, **filters):
        # apply the given filters to self._stmt
        # filters example:
        #   first_name__in=["Alex", "John"]
        #   type__code="sh" - through relation
        #   etc
        for attr, value in filters.items():
            model = self._model
            if "__" in attr:
                for lookup in self._operators:
                    if attr.endswith(f"__{lookup}"):
                        op = self._operators[lookup]
                        attr = attr[:-(len("__") + len(lookup))]
                        break
                else:
                    op = operators.eq
                column = self._extract_column(attr)
            else:
                column = getattr(model, attr)
                op = operators.eq
            self._stmt = self._stmt.where(op(column, value))
        # self._stmt = filtering.filter(self._stmt)
        return self

    def options(self, *args):
        self._options.extend(args)
        return self

    def order_by(self, *ordering_fields):
        # apply ordering
        for field in ordering_fields:
            col = self._extract_column(field.lstrip("-+"))
            col = col.desc() if field.startswith("-") else col.asc()
            self._stmt = self._stmt.order_by(col)
        return self

    def _extract_column(self, field: str):
        # returns SQLAlchemy column by its string representation
        # field example:
        #    type__code - through relation
        #    first_name
        #    etc
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
            joins = self._joins
            if "__" in arg:
                first, rest = arg.split("__", 1)
                start_option_column = getattr(self._model, first)
                option = contains_eager(start_option_column) if first in joins else joinedload(start_option_column)
                joins = joins.get(first, {})
                relationships = inspect(self._model).relationships
                model = relationships[first].mapper.class_
                for i in rest.split("__"):
                    n = getattr(model, i)
                    if i in joins:
                        option = option.contains(n)
                    else:
                        option = option.joinedload(n)
                    relationships = inspect(model).relationships
                    model = relationships[i].mapper.class_
            else:
                start_option_column = getattr(self._model, arg)
                option = contains_eager(start_option_column) if arg in joins else joinedload(start_option_column)
            self._stmt = self._stmt.options(option)

    def _build_stmt(self):
        # builds final statement
        self.apply_options()

    async def all(self):
        self._build_stmt()
        result = await self._session.scalars(self._stmt)
        return result.all()

    async def first(self):
        self._build_stmt()
        stmt = self._stmt.limit(1)
        return await self._session.scalar(stmt)

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
