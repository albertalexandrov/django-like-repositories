import logging
from typing import Any, Type, Self

from sqlalchemy import Select, select, func, delete, Delete, update, Update, inspect
from sqlalchemy.orm import contains_eager, joinedload
from sqlalchemy.sql.operators import eq

from repositories.constants import LOOKUP_SEP
from repositories.lookups import lookups
from repositories.types import Model
from repositories.utils import get_column, get_relationship, get_pk, get_relationships, get_columns, get_annotations

logger = logging.getLogger(__name__)


class InvalidFilteringFieldError(Exception):

    def __init__(self, filter_field: str):
        error = f"Некорректное поле для фильтрации - {filter_field}"
        super().__init__(error)


class InvalidOrderingFieldError(Exception):

    def __init__(self, ordering_field: str):
        error = f"Некорректное поле для сортировки - {ordering_field}"
        super().__init__(error)


class InvalidOptionFieldError(Exception):

    def __init__(self, option_field: str):
        error = f"Некорректное поле для options - {option_field}"
        super().__init__(error)


class InvalidJoinFieldError(Exception):

    def __init__(self, join_field: str):
        error = f"Некорректное поле для join - {join_field}"
        super().__init__(error)


class QueryBuilder:
    """
    Обертка над запросом SQLAlchemy

    Собирает в себя параметры запроса и в конце генерирует запрос
    """

    def __init__(self, model_cls: Type[Model]):
        self._model_cls = model_cls
        self._where = {}
        self._joins = {}
        self._options = {}
        self._ordering = {}
        self._limit = None
        self._offset = None
        self._returning = []
        self._execution_options = {}
        self._values_list = []

    def clone(self) -> Self:
        """
        Создает копию QueryBuilder
        """
        clone = self.__class__(self._model_cls)
        clone._where = {**self._where}
        clone._ordering = {**self._ordering}
        clone._joins = {**self._joins}
        clone._options = {**self._options}
        clone._returning = [*self._returning]
        clone._execution_options = {**self._execution_options}
        clone._values_list = {*self._values_list}
        return clone

    def filter(self, **kw: dict[str:Any]) -> None:
        for filter_field, filter_value in kw.items():
            model_cls = self._model_cls
            column, op = None, eq
            joins = self._joins
            expected = get_annotations(model_cls)
            for attr in filter_field.split(LOOKUP_SEP):
                relationships = get_relationships(model_cls)
                columns = get_columns(model_cls)
                if attr not in expected:
                    raise InvalidFilteringFieldError(filter_field)
                if attr in relationships:
                    relationship = relationships[attr]
                    joins = joins.setdefault("children", {}).setdefault(attr, {})
                    model_cls = relationship.mapper.class_
                    expected = get_annotations(model_cls)
                elif attr in columns:
                    column = columns[attr]
                    expected = lookups
                elif attr in lookups:
                    op = lookups[attr]
                    expected = {}
                else:
                    raise InvalidFilteringFieldError(filter_field)
            assert column is not None
            self._where[filter_field] = op(column, filter_value)

    def order_by(self, *args: str) -> None:
        for ordering_field in args:
            model_cls = self._model_cls
            joins = self._joins
            column = None
            ordering_field = ordering_field.strip("+")
            expected = get_annotations(model_cls)
            for attr in ordering_field.strip("-").split(LOOKUP_SEP):
                relationships = get_relationships(model_cls)
                columns = get_columns(model_cls)
                if attr not in expected:
                    raise InvalidOrderingFieldError(ordering_field)
                if attr in relationships:
                    relationship = relationships[attr]
                    model_cls = relationship.mapper.class_
                    joins = joins.setdefault("children", {}).setdefault(attr, {})
                    expected = get_annotations(model_cls)
                elif attr in columns:
                    column = columns[attr]
                    expected = {}
                else:
                    raise InvalidOrderingFieldError(ordering_field)
            assert column is not None
            self._ordering[ordering_field] = column.desc() if ordering_field.startswith("-") else column.asc()

    def options(self, *args: str) -> None:
        for option_field in args:
            model_cls = self._model_cls
            options = self._options
            joins = self._joins
            relationships = get_relationships(model_cls)
            for attr in option_field.split(LOOKUP_SEP):
                if attr in relationships:
                    joins = joins.setdefault("children", {}).setdefault(attr, {})
                    options = options.setdefault(attr, {})
                    model_cls = relationships[attr].mapper.class_
                    relationships = get_relationships(model_cls)
                else:
                    raise InvalidOptionFieldError(option_field)

    def returning(self, *args: str, return_model: bool = False) -> None:
        # будет учтено только в UPDATE и DELETE запросах
        if args and return_model:
            raise ValueError("args и return_model не могут быть заданы одновременно")
        if not args and not return_model:
            raise ValueError("Задайте либо args, либо return_model")
        self._returning.clear()
        if args:
            for column_name in args:
                column = get_column(self._model_cls, column_name)
                self._returning.append(column)
        if return_model:
            self._returning.append(self._model_cls)

    def execution_options(self, **kw: dict[str, Any]) -> None:
        self._execution_options = kw

    def values_list(self, *args: str) -> None:
        self._values_list.clear()
        for column_name in args:
            column = get_column(self._model_cls, column_name)
            self._values_list.append(column)

    def outerjoin(self, *args: str) -> None:
        for join_field in args:
            model_cls = self._model_cls
            joins = self._joins
            penultimate_joins, last_attr = None, None
            relationships = get_relationships(model_cls)
            for attr in join_field.split(LOOKUP_SEP):
                if attr in relationships:
                    last_attr = attr
                    joins = joins.setdefault("children", {})
                    penultimate_joins = joins
                    joins = joins.setdefault(attr, {})
                    model_cls = relationships[attr].mapper.class_
                    relationships = get_relationships(model_cls)
                else:
                    raise InvalidJoinFieldError(join_field)
            penultimate_joins[last_attr]["isouter"] = True

    def limit(self, limit: int | None) -> None:
        if limit < 1:
            raise ValueError("limit не может быть меньше 1")
        self._limit = limit

    def offset(self, offset: int | None) -> None:
        if offset < 0:
            raise ValueError("offset не можеь быть меньше 0")
        self._offset = offset

    def build_count_stmt(self) -> Select:
        pk = get_pk(self._model_cls)
        stmt = (
            select(func.count(func.distinct(pk)))
            .select_from(self._model_cls)
        )
        stmt = self._apply_joins(stmt)
        # важно сперва применить join-ы и только потом фильтровать
        stmt = self._apply_where(stmt)
        return stmt

    def build_select_stmt(self) -> Select:
        if self._options:
            # в случае, когда заданы options, необходим подзапрос,
            # чтобы корректно подгрузить экземпляры связных моделей
            return self._build_stmt_w_options()
        return self._build_stmt_wo_options()

    def build_delete_stmt(self) -> Delete:
        pk = get_pk(self._model_cls)
        stmt = select(func.distinct(pk))
        stmt = self._apply_execution_options(stmt)
        stmt = self._apply_joins(stmt)
        # важно сперва применить join-ы и только потом фильтровать
        stmt = self._apply_where(stmt)
        stmt = delete(self._model_cls).where(pk.in_(stmt))
        if self._returning:
            stmt = stmt.returning(*self._returning)
        return stmt

    def build_update_stmt(self, values: dict[str, Any]) -> Update:
        pk = get_pk(self._model_cls)
        stmt = select(func.distinct(pk))
        stmt = self._apply_execution_options(stmt)
        stmt = self._apply_joins(stmt, self._joins)
        # важно сперва применить join-ы и только потом фильтровать
        stmt = self._apply_where(stmt)
        stmt = update(self._model_cls).where(pk.in_(stmt)).values(**values)
        if self._returning:
            stmt = stmt.returning(*self._returning)
        return stmt

    def _build_stmt_wo_options(self) -> Select:
        stmt = select(*self._values_list) if self._values_list else select(self._model_cls)
        stmt = self._apply_execution_options(stmt)
        stmt = self._apply_joins(stmt)
        # важно сперва применить join-ы и только потом фильтровать и сортировать
        stmt = self._apply_where(stmt)
        stmt = self._apply_order_by(stmt)
        return stmt

    def _build_stmt_w_options(self) -> Select:
        pk = get_pk(self._model_cls)
        stmt = select(self._model_cls)
        stmt = self._apply_execution_options(stmt)
        stmt = self._apply_joins(stmt)
        # важно сперва применить join-ы и только потом фильтровать и сортировать
        stmt = self._apply_where(stmt)
        subquery = select(func.distinct(pk))
        subquery = self._apply_joins(subquery)
        subquery = self._apply_where(subquery)
        subquery = self._apply_limit(subquery)
        subquery = self._apply_offset(subquery)
        stmt = stmt.where(pk.in_(subquery))
        stmt = self._apply_order_by(stmt)
        stmt = self._apply_options(stmt)
        return stmt

    def _apply_execution_options(self, stmt: Select) -> Select:
        return stmt.execution_options(**self._execution_options)

    def _apply_options(self, stmt: Select) -> Select:
        for relationship_names in self._flat_options(self._options):
            joins = self._joins["children"]
            option = None
            model_cls = self._model_cls
            for relationship_name in relationship_names:
                relationship = get_relationship(model_cls, relationship_name)
                class_attribute = relationship.class_attribute
                if relationship_name in joins:
                    option = (
                        option.contains_eager(class_attribute)
                        if option
                        else contains_eager(class_attribute)
                    )
                else:
                    option = (
                        option.joinedload(class_attribute, innerjoin=True) if option else joinedload(class_attribute, innerjoin=True)
                    )
                joins = joins.get(relationship_name, {}).get("children", {})
                model_cls = relationship.mapper.class_
            stmt = stmt.options(option)
        return stmt

    def _flat_options(self, options: dict[str, dict], flattened: list = None) -> list[str, ...]:
        if flattened is None:
            flattened = []
        result = []
        for key, value in options.items():
            new_prefix = flattened + [key]
            if isinstance(value, dict) and not value:
                result.append(new_prefix)
            elif isinstance(value, dict):
                result.extend(self._flat_options(value, new_prefix))
        return result

    def _apply_offset(self, stmt: Select) -> Select:
        if self._offset is not None:
            stmt = stmt.offset(self._offset)
        return stmt

    def _apply_limit(self, stmt: Select) -> Select:
        if self._limit is not None:
            stmt = stmt.limit(self._limit)
        return stmt

    def _apply_where(self, stmt: Select) -> Select:
        return stmt.where(*self._where.values())

    def _apply_order_by(self, stmt: Select) -> Select:
        return stmt.order_by(*self._ordering.values())

    def _apply_joins(self, stmt: Select, model_cls=None, joins: dict = None) -> Select:
        # todo: проjoinить одну таблицу несколько раз не получится
        model_cls = model_cls or self._model_cls
        joins = self._joins if joins is None else joins
        for join, value in joins.get("children", {}).items():
            isouter = value.get("isouter", False)
            relationship = get_relationship(model_cls, join)
            stmt = stmt.join(relationship.class_attribute, isouter=isouter)
            stmt = self._apply_joins(stmt, relationship.mapper.class_, value)
        return stmt
