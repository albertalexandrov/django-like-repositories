import logging
from typing import Any, Type, Self

from sqlalchemy import Select, select, func, delete, Delete, update, Update
from sqlalchemy.orm import contains_eager, joinedload, aliased
from sqlalchemy.sql.operators import eq

from models import Section, Subsection, PublicationStatus
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

    рутовая модель пусть всегда будет безалиасной?
    _where
        в рутовой модели. просто список? ['name', 'status_id']
        брать колонки от модели или алиаса
    _order_by
        тоже самое как для _where

    _joins
        при джойнах не всегда нужно выполнять сортировку (в случае с _options с подзапросом)

    """

    def __init__(self, model_cls: Type[Model]):
        self._model_cls = model_cls
        self._where = {'name': {'op': eq, 'value': "значение"}}
        self._joins = {
            "children": {
                "subsections": {
                    "model_cls": Subsection,
                    "where": {
                        'name': {
                            'op': eq,
                            'value': "значение"
                        }
                    },
                    "order_by": {
                        'status_id': {
                            'direction': 'asc'
                        },
                        'name': {
                            'direction': 'desc'
                        },
                    },
                    "is_outer": False,
                    "children": {
                        "status": {
                            'model_cls': PublicationStatus,
                            "where": {
                                'code': {
                                    'op': eq,
                                    'value': "published"
                                }
                            },
                        }
                    }
                },
                "status": {
                    "model_cls": PublicationStatus,
                    "is_outer": False,
                }
            }
        }
        self._options = ["subsections__status", "status"]  # {"subsections": {"status": {}}, "status": {}}
        self._order_by = {
            'status_id': {
                'direction': 'asc'
            },
            'name': {
                'direction': 'desc'
            },
        }
        self._limit = None
        self._offset = None
        self._returning = []
        self._execution_options = {}
        self._values_list = []
        self._last_options = {}

    def clone(self) -> Self:
        """
        Создает копию QueryBuilder
        """
        clone = self.__class__(self._model_cls)
        clone._where = {**self._where}
        clone._order_by = {**self._order_by}
        clone._joins = {**self._joins}
        clone._options = [*self._options]
        clone._returning = [*self._returning]
        clone._execution_options = {**self._execution_options}
        clone._values_list = [*self._values_list]
        clone._limit = self._limit
        clone._offset = self._offset
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
                    joins = joins.setdefault("children", {})
                    joins, model_cls = self._join(relationship, attr, joins)
                    expected = get_annotations(model_cls)
                elif attr in columns:
                    column = getattr(model_cls, attr)
                    expected = lookups
                elif attr in lookups:
                    op = lookups[attr]
                    expected = {}
                else:
                    raise InvalidFilteringFieldError(filter_field)
            if column is None:
                raise InvalidFilteringFieldError(filter_field)
            self._where[filter_field] = op(column, filter_value)
        print(self._joins)

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
                    joins = joins.setdefault("children", {})
                    joins, model_cls = self._join(relationship, attr, joins)
                    expected = get_annotations(model_cls)
                elif attr in columns:
                    column = getattr(model_cls, attr)
                    expected = {}
                else:
                    raise InvalidOrderingFieldError(ordering_field)
            if column is None:
                raise InvalidOrderingFieldError(ordering_field)
            self._order_by[ordering_field] = column.desc() if ordering_field.startswith("-") else column.asc()

    def options(self, *args: str) -> None:
        for option_field in args:
            model_cls = self._model_cls
            options = self._options
            joins = self._joins
            relationships = get_relationships(model_cls)
            for attr in option_field.split(LOOKUP_SEP):
                if attr in relationships:
                    relationship = relationships[attr]
                    options = options.setdefault(attr, {})
                    joins = joins.setdefault("children", {})
                    joins, model_cls = self._join(relationship, attr, joins)
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
                    relationship = relationships[attr]
                    joins = joins.setdefault("children", {})
                    penultimate_joins = joins
                    joins, model_cls = self._join(relationship, attr, joins)
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
        stmt = self._apply_joins(stmt)
        # важно сперва применить join-ы и только потом фильтровать
        stmt = self._apply_where(stmt)
        stmt = update(self._model_cls).where(pk.in_(stmt)).values(**values)
        if self._returning:
            stmt = stmt.returning(*self._returning)
        return stmt

    def _join(self, relationship, attr: str, joins: dict[str, Any]) -> tuple[dict, Model]:
        if attr in joins:
            joins = joins[attr]
            model_cls = joins["target"]
        else:
            model_cls = aliased(relationship.mapper.class_)
            joins = joins.setdefault(attr, {})
            joins["target"] = model_cls
            joins["onclause"] = relationship.class_attribute
        return joins, model_cls

    def _build_stmt_wo_options(self) -> Select:
        stmt = select(*self._values_list) if self._values_list else select(self._model_cls)
        stmt = self._apply_execution_options(stmt)
        stmt = self._apply_joins_new(stmt)
        # важно сперва применить join-ы и только потом фильтровать и сортировать
        stmt = self._apply_where(stmt)
        stmt = self._apply_order_by(stmt)
        return stmt

    def _build_stmt_w_options(self) -> Select:
        """
        Работает верно:
        SELECT anon_1.id,
               anon_1.name,
               anon_1.status_id,
               subsections.id        AS id_1,
               subsections.name      AS name_1,
               subsections.section_id,
               subsections.status_id AS status_id_1
        FROM (
            SELECT DISTINCT sections.id AS id, sections.name AS name, sections.status_id AS status_id
            FROM sections
            LEFT JOIN subsections ON sections.id = subsections.section_id AND subsections.status_id = 1
            LIMIT 10
        ) AS anon_1
        LEFT JOIN subsections ON anon_1.id = subsections.section_id AND subsections.status_id = 1

        Нужно:
        1. создать подзапрос
        """
        if self._limit or self._offset:
            # надо делать подзапрос
            # жойны в подзапросе и внешнем запросе сохраняются
            subquery = select(self._model_cls).distinct()
            subquery = self._apply_limit(subquery)
            subquery = self._apply_offset(subquery)
            subquery = self._apply_where(subquery)
            subquery = self._apply_joins_new(subquery, apply_order_by=False, apply_options=False)
            SectionAlias = aliased(self._model_cls, subquery.subquery())
            stmt = select(SectionAlias)
            stmt = self._apply_joins_new(stmt, parent_model_cls=SectionAlias)
        else:
            # селектится все
            # не нужно делать подзапрос
            stmt = select(self._model_cls)
            stmt = self._apply_joins_new(stmt)
            stmt = self._apply_where(stmt)
            stmt = self._apply_order_by(stmt)

        return stmt

    def _apply_execution_options(self, stmt: Select) -> Select:
        return stmt.execution_options(**self._execution_options)

    def _apply_options(self, stmt: Select, model_cls=None) -> Select:
        for relationship_names in self._flat_options(self._options):
            option = None
            model_cls = self._model_cls if model_cls is None else model_cls
            for relationship_name in relationship_names:
                onclause = getattr(model_cls, relationship_name)
                option = (
                    option.contains_eager(onclause)
                    if option
                    else contains_eager(onclause)
                )
                model_cls = onclause.property.mapper.class_
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

    def _apply_where(self, stmt: Select, where: dict = None, model_cls=None) -> Select:
        model_cls = self._model_cls if model_cls is None else model_cls
        where = self._where if where is None else where
        for attr, value in where.items():
            op = value['op']
            column = getattr(model_cls, attr)  # напр., aliased(Section).name или Section.name
            stmt = stmt.where(op(column, value['value']))
        return stmt

    def _apply_order_by(self, stmt: Select, order_by: dict = None, model_cls=None) -> Select:
        model_cls = self._model_cls if model_cls is None else model_cls
        order_by = self._order_by if order_by is None else order_by
        for attr, value in order_by.items():
            direction = value['direction']
            column = getattr(model_cls, attr)  # напр., aliased(Section).name или Section.name
            column = column.asc() if direction == 'asc' else column.desc()
            stmt = stmt.order_by(column)
        return stmt

    def _apply_joins(self, stmt: Select, model_cls: Type[Model] = None, joins: dict = None) -> Select:
        model_cls = model_cls or self._model_cls
        joins = self._joins if joins is None else joins
        for relationship_name, value in joins.get("children", {}).items():
            target = value["target"]
            onclause = value["onclause"]
            isouter = value.get("isouter", False)
            relationship = get_relationship(model_cls, relationship_name)
            stmt = stmt.join(target, onclause, isouter=isouter)
            stmt = self._apply_joins(stmt, relationship.mapper.class_, value)
        return stmt

    def _apply_joins_new(
            self,
            stmt: Select,
            apply_where: bool = True,
            apply_order_by: bool = True,
            apply_options: bool = True,
            parent_model_cls=None
    ) -> Select:
        """
        как сейчас:

        {
            'children': {
                'subsections': {
                    'target': < AliasedClass at 0x118c9c650;Subsection > ,
                    'onclause': < sqlalchemy.orm.attributes.InstrumentedAttribute object at 0x118ba1260 > ,
                    'children': {
                        'status': {
                            'target': < AliasedClass at 0x118cba790;PublicationStatus > ,
                            'onclause': < sqlalchemy.orm.attributes.InstrumentedAttribute object at 0x118ba22a0 >
                        }
                    }
                }
            }
        }

        _where (к рут модели)
        _order_by (к рут модели)

        рут модель может быть обычной моделью или алиасом

        join-ы:
        {
            "children": {
                "subsections": {
                    "model_cls": Subsection,
                    "where": ['code', 'name'],
                    "order_by": ['status_id'],
                    "is_outer": False,
                    "children": {
                        "status": {
                            'model_cls': PublicationStatus,
                        }
                    }
                }
            }
        }
        apply joins это про применение жойнов и связанных с ним фильтров и сортировок
        но у основной модели свои фильтры и сортировки, которые применяются отдельно - и это должны быть строки,
        тк внешней моделью может быть алиас, а не рут модель


        """
        parent_model_cls = self._model_cls if parent_model_cls is None else parent_model_cls
        joins = self._joins
        """
        {
            "children": {
                "subsections": {
                    "model_cls": aliased(Section),
                    "children": {
                        "status": {
                            "model_cls": aliased(Subsection),
                            "children": {
                            
                            }
                        }
                    }
                }
            }
        }
        """

        """
        {
            "children": {
                "subsections": {
                    "model_cls": Subsection,
                    "where": {
                        'name': {
                            'op': eq,
                            'value': "значение"
                        }
                    },
                    "order_by": {
                        'status_id': {
                            'direction': 'asc'
                        },
                        'name': {
                            'direction': 'desc'
                        },
                    },
                    "is_outer": False,
                    "children": {
                        "status": {
                            'model_cls': PublicationStatus,
                            "where": {
                                'code': {
                                    'op': eq,
                                    'value': "published"
                                }
                            },
                        }
                    }
                },
                "status": {
                    "model_cls": PublicationStatus,
                    "is_outer": False,
                }
            }
        }
        """
        where = []
        order_by = []
        tree = {}
        def recursive(stmt, joins, where, order_by, parent_model_cls, tree, root):
            for attr, value in joins.get("children", {}).items():
                target = aliased(value["model_cls"])
                onclause = getattr(parent_model_cls, attr)
                attr_root = f"{root}__{attr}".strip("__")
                tree[attr_root] = {"attr": onclause, "alias": target}
                isouter = value.get("is_outer", False)
                stmt = stmt.join(target, onclause, isouter=isouter)
                for name, item in value.get("where", {}).items():
                    op = item["op"]
                    column = getattr(target, name)
                    where.append(op(column, item["value"]))
                for name, item in value.get("order_by", {}).items():
                    direction = item["direction"]
                    column = getattr(target, name)
                    order_by.append(column.asc() if direction == 'asc' else column.desc())
                stmt = recursive(
                    stmt,
                    joins=value,
                    order_by=order_by,
                    where=where,
                    parent_model_cls=target,
                    tree=tree,
                    root=attr_root
                )
            return stmt

        stmt = recursive(
            stmt=stmt, joins=joins, where=where, order_by=order_by, parent_model_cls=parent_model_cls, tree=tree,
            root=""
        )
        print(stmt)
        print(order_by)
        print(where)
        print(tree)
        if apply_where:
            stmt = stmt.where(*where)
        if apply_order_by:
            stmt = stmt.order_by(*order_by)
        if apply_options:
            options = self._get_options(tree)
            stmt = stmt.options(*options)
        return stmt

    def _get_options(self, tree):
        options = []
        for option_field in self._options:
            option = None
            parts = option_field.split('__')
            result = []
            for i in range(1, len(parts) + 1):
                result.append('__'.join(parts[:i]))
            for attr in result:
                data = tree[attr]
                if option:
                    option = option.contains_eager(attr=data["attr"], alias=data["alias"])
                else:
                    option = contains_eager(data["attr"].of_type(data["alias"]))
            options.append(option)
        return options
