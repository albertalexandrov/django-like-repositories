import logging
from typing import Any, Type, Self

from sqlalchemy import Select, select, func, delete, Delete, update, Update
from sqlalchemy.orm import contains_eager, aliased
from sqlalchemy.sql.operators import eq

from repositories.constants import LOOKUP_SEP
from repositories.lookups import lookups
from repositories.types import Model
from repositories.utils import get_column, get_pk, get_relationships, get_columns, get_annotations

logger = logging.getLogger(__name__)


class InvalidFilterFieldError(Exception):

    def __init__(self, filter_field: str):
        error = f"Некорректное поле для фильтрации - {filter_field}"
        super().__init__(error)


class InvalidOrderByFieldError(Exception):

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
        self._where = {}
        # self._where = {'name': {'op': eq, 'value': "значение"}}
        self._joins = {}
        # self._joins = {
        #     "children": {
        #         "subsections": {
        #             "model_cls": Subsection,
        #             "where": {
        #                 'name': {
        #                     'op': eq,
        #                     'value': "значение"
        #                 }
        #             },
        #             "order_by": {
        #                 'status_id': {
        #                     'direction': 'asc'
        #                 },
        #                 'name': {
        #                     'direction': 'desc'
        #                 },
        #             },
        #             "is_outer": False,
        #             "children": {
        #                 "status": {
        #                     'model_cls': PublicationStatus,
        #                     "where": {
        #                         'code': {
        #                             'op': eq,
        #                             'value': "published"
        #                         }
        #                     },
        #                 }
        #             }
        #         },
        #         "status": {
        #             "model_cls": PublicationStatus,
        #             "is_outer": False,
        #         }
        #     }
        # }
        # self._options = ["subsections__status", "status"]  # {"subsections": {"status": {}}, "status": {}}
        self._options = set()
        self._order_by = {}
        # self._order_by = {
        #     'status_id': {
        #         'direction': 'asc'
        #     },
        #     'name': {
        #         'direction': 'desc'
        #     },
        # }
        self._limit = None
        self._offset = None
        self._returning = []
        self._execution_options = {}
        self._select_entities = []
        self._distinct = None

    def clone(self) -> Self:
        """
        Создает копию QueryBuilder
        """
        clone = self.__class__(self._model_cls)
        clone._where = {**self._where}
        clone._order_by = {**self._order_by}
        clone._joins = {**self._joins}
        clone._options = {*self._options}
        clone._returning = [*self._returning]
        clone._execution_options = {**self._execution_options}
        clone._select_entities = [*self._select_entities]
        clone._limit = self._limit
        clone._offset = self._offset
        clone._distinct = self._distinct
        return clone

    def filter(self, **kw: dict[str:Any]) -> None:
        for filter_field, filter_value in kw.items():
            model_cls = self._model_cls
            column_name, op = None, eq
            joins = self._joins
            expected = get_annotations(model_cls)
            where = self._where
            column_name = filter_field
            for attr in filter_field.split(LOOKUP_SEP):
                relationships = get_relationships(model_cls)
                columns = get_columns(model_cls)
                if attr not in expected:
                    raise InvalidFilterFieldError(filter_field)
                if attr in relationships:
                    model_cls = getattr(model_cls, attr).property.mapper.class_
                    joins = joins.setdefault("children", {}).setdefault(attr, {})
                    joins['model_cls'] = model_cls
                    if "where" in joins:
                        where = joins["where"]
                    else:
                        joins["where"] = {}
                        where = joins["where"]
                    expected = get_annotations(model_cls)
                elif attr in columns:
                    column_name = attr
                    expected = lookups
                elif attr in lookups:
                    op = lookups[attr]
                    expected = {}
                else:
                    raise InvalidFilterFieldError(filter_field)
            if not column_name:
                raise InvalidFilterFieldError(filter_field)
            where[column_name] = {"op": op, "value": filter_value}

    def order_by(self, *args: str) -> None:
        for ordering_field in args:
            model_cls = self._model_cls
            joins = self._joins
            column_name = None
            order_by = self._order_by
            ordering_field = ordering_field.strip("+")
            expected = get_annotations(model_cls)
            for attr in ordering_field.strip("-").split(LOOKUP_SEP):
                relationships = get_relationships(model_cls)
                columns = get_columns(model_cls)
                if attr not in expected:
                    raise InvalidOrderByFieldError(ordering_field)
                if attr in relationships:
                    model_cls = getattr(model_cls, attr).property.mapper.class_
                    joins = joins.setdefault("children", {}).setdefault(attr, {})
                    joins['model_cls'] = model_cls
                    if "order_by" in joins:
                        order_by = joins["order_by"]
                    else:
                        joins["order_by"] = {}
                        order_by = joins["order_by"]
                    expected = get_annotations(model_cls)
                elif attr in columns:
                    column_name = attr
                    expected = {}
                else:
                    raise InvalidOrderByFieldError(ordering_field)
            if column_name is None:
                raise InvalidOrderByFieldError(ordering_field)
            order_by[column_name] = {
                "direction": "desc" if ordering_field.startswith("-") else "asc"
            }

    def options(self, *args: str) -> None:
        for option_field in args:
            model_cls = self._model_cls
            joins = self._joins
            relationships = get_relationships(model_cls)
            for attr in option_field.split(LOOKUP_SEP):
                if attr in relationships:
                    model_cls = getattr(model_cls, attr).property.mapper.class_
                    joins = joins.setdefault("children", {}).setdefault(attr, {})
                    joins['model_cls'] = model_cls
                    relationships = get_relationships(model_cls)
                else:
                    raise InvalidOptionFieldError(option_field)
            self._options.add(option_field)

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
        self._select_entities.clear()
        for column_name in args:
            column = get_column(self._model_cls, column_name)
            self._select_entities.append(column)

    def join(self, *args: str, isouter: bool) -> None:
        for join_field in args:
            model_cls = self._model_cls
            joins = self._joins
            relationships = get_relationships(model_cls)
            for attr in join_field.split(LOOKUP_SEP):
                if attr in relationships:
                    model_cls = getattr(model_cls, attr).property.mapper.class_
                    joins = joins.setdefault("children", {}).setdefault(attr, {})
                    joins['model_cls'] = model_cls
                    relationships = get_relationships(model_cls)
                else:
                    raise InvalidJoinFieldError(join_field)
            joins["isouter"] = isouter

    def distinct(self) -> None:
        self._distinct = True

    def limit(self, limit: int | None) -> None:
        if limit < 1:
            raise ValueError("limit не может быть меньше 1")
        self._limit = limit

    def offset(self, offset: int | None) -> None:
        if offset < 0:
            raise ValueError("offset не можеь быть меньше 0")
        self._offset = offset

    def build_count_stmt(self) -> Select:
        if self._options:
            raise ValueError("Удалите options")
        pk = get_pk(self._model_cls)
        stmt = (
            select(func.count(func.distinct(pk)))
            .select_from(self._model_cls)
        )
        stmt = self._apply_joins(stmt)
        stmt = self._apply_where(stmt)
        return stmt

    def build_delete_stmt(self) -> Delete:
        if self._options:
            raise ValueError("Удалите options")
        pk = get_pk(self._model_cls)
        stmt = select(func.distinct(pk))
        stmt = self._apply_execution_options(stmt)
        stmt = self._apply_joins(stmt)
        stmt = self._apply_where(stmt)
        stmt = delete(self._model_cls).where(pk.in_(stmt))
        stmt = self._apply_returning(stmt)
        return stmt

    def build_update_stmt(self, values: dict[str, Any]) -> Update:
        if self._options:
            raise ValueError("Удалите options")
        pk = get_pk(self._model_cls)
        stmt = select(func.distinct(pk))
        stmt = self._apply_execution_options(stmt)
        stmt = self._apply_joins(stmt)
        stmt = self._apply_where(stmt)
        stmt = update(self._model_cls).where(pk.in_(stmt)).values(**values)
        stmt = self._apply_returning(stmt)
        return stmt

    def _apply_returning(self, stmt: Update | Delete) -> Update | Delete:
        if self._returning:
            stmt = stmt.returning(*self._returning)
        return stmt

    def _apply_distinct(self, stmt: Select) -> Select:
        if self._distinct is True:
            stmt = stmt.distinct()
        return stmt

    def build_select_stmt(self) -> Select:
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
        if self._options and self._select_entities:
            raise ValueError("Одновременно заданные options и values_list не могут быть обработаны вместе")
        if self._options and (self._limit or self._offset):
            # надо делать подзапрос
            # жойны в подзапросе и внешнем запросе сохраняются
            subquery = select(self._model_cls)
            subquery = subquery.distinct()
            subquery = self._apply_limit(subquery)
            subquery = self._apply_offset(subquery)
            subquery = self._apply_where(subquery)
            subquery = self._apply_order_by(subquery)
            subquery = self._apply_joins(subquery, apply_options=False)
            AliasedModelCls = aliased(self._model_cls, subquery.subquery())
            stmt = select(AliasedModelCls)
            stmt = self._apply_distinct(stmt)
            stmt = self._apply_joins(stmt, parent_model_cls=AliasedModelCls)
        else:
            # селектится все
            # не нужно делать подзапрос
            stmt = select(*self._select_entities) if self._select_entities else select(self._model_cls)
            stmt = self._apply_execution_options(stmt)
            stmt = self._apply_distinct(stmt)
            stmt = self._apply_joins(stmt)
            stmt = self._apply_where(stmt)
            stmt = self._apply_order_by(stmt)
            stmt = self._apply_limit(stmt)
            stmt = self._apply_offset(stmt)
        return stmt

    def _apply_execution_options(self, stmt: Select) -> Select:
        return stmt.execution_options(**self._execution_options)

    def _apply_offset(self, stmt: Select) -> Select:
        if self._offset is not None:
            stmt = stmt.offset(self._offset)
        return stmt

    def _apply_limit(self, stmt: Select) -> Select:
        if self._limit is not None:
            stmt = stmt.limit(self._limit)
        return stmt

    def _apply_where(self, stmt, model_cls=None) -> Select:
        model_cls = model_cls or self._model_cls
        for attr, value in self._where.items():
            op = value['op']
            column = getattr(model_cls, attr)
            stmt = stmt.where(op(column, value['value']))
        return stmt

    def _apply_order_by(self, stmt: Select, model_cls=None):
        model_cls = model_cls or self._model_cls
        for attr, value in self._order_by.items():
            direction = value['direction']
            column = getattr(model_cls, attr)  # напр., aliased(Section).name или Section.name
            column = column.asc() if direction == 'asc' else column.desc()
            stmt = stmt.order_by(column)
        return stmt

    def _apply_joins(
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
        stmt = self._apply_joins_recursively(
            stmt=stmt,
            joins=joins,
            where=where,
            order_by=order_by,
            parent_model_cls=parent_model_cls,
            tree=tree,
            root="",
        )
        if apply_where:
            stmt = stmt.where(*where)
        if apply_order_by:
            stmt = stmt.order_by(*order_by)
        if apply_options:
            stmt = self._apply_options(stmt, tree)
        return stmt

    def _apply_joins_recursively(self, stmt, joins, where, order_by, parent_model_cls, tree, root):
        for attr, value in joins.get("children", {}).items():
            target = aliased(value["model_cls"])
            onclause = getattr(parent_model_cls, attr)
            attr_root = f"{root}__{attr}".strip("__")
            tree[attr_root] = {"attr": onclause, "alias": target}
            isouter = value.get("isouter", False)
            stmt = stmt.join(target, onclause, isouter=isouter)
            for name, item in value.get("where", {}).items():
                op = item["op"]
                column = getattr(target, name)
                where.append(op(column, item["value"]))
            for name, item in value.get("order_by", {}).items():
                direction = item["direction"]
                column = getattr(target, name)
                order_by.append(column.asc() if direction == 'asc' else column.desc())
            stmt = self._apply_joins_recursively(
                stmt,
                joins=value,
                order_by=order_by,
                where=where,
                parent_model_cls=target,
                tree=tree,
                root=attr_root
            )
        return stmt

    def _apply_options(self, stmt: Select, tree: dict) -> Select:
        for option_field in self._options:
            option = None
            attrs = option_field.split(LOOKUP_SEP)
            keys = []
            for i in range(1, len(attrs) + 1):
                keys.append(LOOKUP_SEP.join(attrs[:i]))
            for attr in keys:
                data = tree[attr]
                if option:
                    option = option.contains_eager(attr=data["attr"], alias=data["alias"])
                else:
                    option = contains_eager(data["attr"].of_type(data["alias"]))
            stmt = stmt.options(option)
        return stmt
