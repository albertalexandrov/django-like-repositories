import logging
from typing import Any, Literal

from sqlalchemy import inspect, Column, Select, select, func, Executable, delete, Delete, update, Update
from sqlalchemy.orm import Relationship, contains_eager, joinedload, aliased
from sqlalchemy.sql.operators import eq

from repositories.constants import LOOKUP_SEP
from repositories.exceptions import ColumnNotFoundError, RelationshipNotFoundError
from repositories.lookups import lookups
from repositories.utils import validate_has_columns, get_column

logger = logging.getLogger(__name__)

counter = 0

class QueryBuilder:
    def __init__(self, model_cls):
        self._model_cls = model_cls
        self._where = {}
        self._joins = {}
        self._options = {}
        self._order_by = {}
        self._limit = None
        self._offset = None
        self._returning = []
        self._execution_options = None
        self._values_list = []

    def filter(self, **kwargs: dict[str:Any]) -> None:
        """
        Обрабатывает условия фильтрации и сопутствующие join-ы

        :param kwargs: условия фильтрации
        """
        # предполагаем, что в последней позиции названия фильтра filter_name находится lookup
        lookup_expected_idx = -1
        for filter_name, filter_value in kwargs.items():
            if filter_name in self._where:
                logger.warning(f"Фильтр {filter_name} уже был применен ранее")
            if LOOKUP_SEP in filter_name:
                # здесь будет происходить определение модели и столбца (column_name),
                # по которому нужно выполнить фильтрацию
                attrs = filter_name.split(LOOKUP_SEP)
                if attrs[lookup_expected_idx] == attrs[lookup_expected_idx - 1]:
                    # название lookup-а и столбца могут совпадать.  например, для модели:
                    #
                    #   class Meet:
                    #       title: Mapped[str]
                    #       day: Mapped[date]
                    #
                    # может понадобиться выполнить фильтрацию по дню недели фильтром day__day
                    lookup = attrs.pop()
                else:
                    lookup = attrs.pop() if attrs[lookup_expected_idx] in lookups else 'exact'
                if not (op := lookups.get(lookup)):
                    raise ValueError(f"lookup `{lookup}` не найден")
                column = self._extract_column(attrs, filter_name, 'filter')
            else:
                op = eq
                column = get_column(self._model_cls, filter_name)
            self._where[filter_name] = op(column, filter_value)

    def limit(self, limit: int | None) -> None:
        """
        Сохраняет ограничение количества строк

        :param limit: количество строк
        """
        if limit < 1:
            raise ValueError("limit не может быть меньше 1")
        self._limit = limit

    def offset(self, offset: int | None) -> None:
        """
        Сохраняет смещение

        :param offset: смещение
        """
        if offset < 0:
            raise ValueError("offset не можеь быть меньше 0")
        self._offset = offset

    def order_by(self, *args: str) -> None:
        """
        Обрабатывает условия сортировки и сопутствующие join-ы

        :param args: условия фильтрации
        """
        for ordering_name in args:
            if ordering_name in self._order_by:
                logger.warning(f"По полю `{ordering_name}` ранее уже была применена сортировка")
            column = self._extract_column(ordering_name.lstrip("+-").split(LOOKUP_SEP), ordering_name, 'order_by')
            column = column.desc() if ordering_name.startswith("-") else column.asc()
            self._order_by[ordering_name] = column

    def options(self, *args: str) -> None:
        """
        Обрабатывает options для подгрузки экземпляров связных моделей

        :param args: options
        """
        for option_path in args:
            model = self._model_cls
            options = self._options
            joins = self._joins
            for attr in option_path.split(LOOKUP_SEP):
                relationship = self._get_relationship(model, attr, raise_=True)
                options = options.setdefault(attr, {})
                joins = joins.setdefault("children", {}).setdefault(attr, {})
                model = relationship.mapper.class_

    def returning(self, *cols, return_model: bool = False) -> None:
        """
        Сохраняет информацию о том, что нужно вернуть в ходе выполнения запроса

        :param cols:
        :param return_model:
        """
        if cols and return_model:
            raise ValueError("Запрещено одновременно задать cols и return_model")
        self._returning.clear()
        if cols:
            for col in cols:
                column = get_column(self._model_cls, col)
                self._returning.append(column)
        if return_model:
            self._returning.append(self._model_cls)

    def execution_options(self, **kwargs: dict[str:Any]) -> None:
        self._execution_options = kwargs

    def values_list(self, *args: str) -> None:
        validate_has_columns(self._model_cls, *args)
        self._values_list.clear()
        for field in args:
            self._values_list.append(get_column(self._model_cls, field))

    def build_count_stmt(self) -> Select:
        """
        Возвращает запрос на подсчет количества записей
        """
        stmt = (
            select(func.count(func.distinct(*self._get_model_pk())))
            .select_from(self._model_cls)
        )
        stmt = self._apply_joins(stmt)
        stmt = self._apply_where(stmt)
        return stmt

    def build_select_stmt(self) -> Select:
        stmt = select(*self._values_list) if self._values_list else select(self._model_cls)
        stmt = self._apply_execution_options(stmt)
        stmt = self._apply_joins(stmt)
        stmt = self._apply_where(stmt)
        if self._options:
            subquery = select(func.distinct(*self._get_model_pk()))
            subquery = self._apply_joins(subquery)
            subquery = self._apply_where(subquery)
            subquery = self._apply_order_by(subquery)
            subquery = self._apply_limit(subquery)
            subquery = self._apply_offset(subquery)
            stmt = stmt.where(self._model_cls.id.in_(subquery))
            stmt = self._apply_options(stmt)  # должно быть именно тут
        else:
            stmt = self._apply_order_by(stmt)
            stmt = self._apply_limit(stmt)
            stmt = self._apply_offset(stmt)
        return stmt

    def build_delete_stmt(self) -> Delete:
        """
        Возвращает запрос на удаление
        """
        stmt = select(func.distinct(self._model_cls.id))
        stmt = self._apply_joins(stmt)
        stmt = self._apply_where(stmt)
        stmt = delete(self._model_cls).where(self._model_cls.id.in_(stmt))
        if self._returning:
            stmt = stmt.returning(*self._returning)
        return stmt

    def build_update_stmt(self, values: dict[str:Any]) -> Update:
        stmt = select(func.distinct(self._model_cls.id))
        stmt = self._apply_joins(stmt, self._joins)
        stmt = self._apply_where(stmt)
        stmt = (
            update(self._model_cls)
            .where(self._model_cls.id.in_(stmt))
            .values(**values)
        )
        if self._returning:
            stmt = stmt.returning(*self._returning)
        return stmt

    def join(self, *joins: str, isouter: bool) -> None:
        for join in joins:
            model = self._model_cls
            joins_tree = self._joins
            prev, last_join_attr = None, None
            for attr in join.split(LOOKUP_SEP):
                relationship = self._get_relationship(model, attr, True)
                last_join_attr = attr
                joins_tree = joins_tree.setdefault("children", {})
                prev = joins_tree
                joins_tree = joins_tree.setdefault(attr, {})
                model = relationship.mapper.class_
            prev[last_join_attr]["isouter"] = isouter

    def _apply_execution_options(self, stmt):
        execution_options = self._execution_options or {}
        stmt = stmt.execution_options(**execution_options)
        return stmt

    def _apply_options(self, stmt: Select) -> Select:
        """
        Применяет к запросу options
        :param stmt: запрос
        """
        for relationship_names in self._flat_options(self._options):
            joins = self._joins["children"]
            option = None
            model = self._model_cls
            for relationship_name in relationship_names:
                relationship = self._get_relationship(model, relationship_name, raise_=True)
                class_attribute = relationship.class_attribute
                if relationship_name in joins:
                    option = (
                        option.contains_eager(class_attribute)
                        if option
                        else contains_eager(class_attribute)
                    )
                else:
                    option = (
                        option.joinedload(class_attribute) if option else joinedload(class_attribute)
                    )
                joins = joins.get(relationship_name, {}).get("children", {})
                model = relationship.mapper.class_
            stmt = stmt.options(option)
        return stmt

    def _flat_options(self, options: dict[str:dict], flattened: list = None) -> list[str, ...]:
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

    def _extract_column(self, attrs: list[str], field_name: str, op: Literal['order_by', 'filter']) -> Column:
        """
        Извлекает из строки вида column1__relationship__column2 столбец SQLAlchemy
        :param attrs:
        :param field_name:
        :param op:
        """
        model = self._model_cls
        joins = self._joins
        column_name = None
        for idx, column_or_relationship_name in enumerate(attrs):
            # необходимо пройти все column_or_relationship_name, чтобы проверить валидность фильтра
            # рассмотрим пример.  пусть есть модель:
            #
            #   class User(Base):
            #       first_name: Mapped[str]
            #       last_name: Mapped[str]
            #
            # и пусть пользователь задал фильтр first_name__last_name
            # тогда, если закончить на первом найденном столбце - first_name, то, во-первых,
            # пользователь не будет знать, что неверно сформировал фильтр, и, во-вторых, он
            # может получить неожидаемый результат, тк возможно он хотел отфильтровать по last_name
            if relationship := self._get_relationship(model, column_or_relationship_name):
                joins = joins.setdefault("children", {})
                joins = joins.setdefault(column_or_relationship_name, {})
                if idx == len(attrs) - 1:
                    raise ColumnNotFoundError(model, column_or_relationship_name)
                model = relationship.mapper.class_
            else:
                validate_has_columns(model, column_or_relationship_name)
                if column_name is not None:
                    # пусть есть модель:
                    #
                    #   class User(Base):
                    #       first_name: Mapped[str]
                    #       last_name: Mapped[str]
                    #
                    # и пусть пользователь задал фильтр first_name__last_name.  тогда, если не выполнить
                    # проверку, будет выполнена фильтрация по последнему валидному полю - last_name
                    if op == 'filter':
                        raise ValueError(
                            f"`В поле {field_name}` указано несколько столбцов модели "
                            f"{model.__name__} для фильтрации, что не дает однозначно понять, "
                            "по какому именно столбцу необходимо выполнить фильтрацию"
                        )
                    elif op == 'order_by':
                        raise ValueError(
                            f"`В поле {field_name}` указано несколько столбцов модели "
                            f"{model.__name__} для сортировки, что не дает однозначно понять, "
                            "по какому именно столбцу необходимо выполнить сортировку"
                        )
                    else:
                        raise ValueError(f"Неожиданное значение параметра `op` - `{op}`")
                column_name = column_or_relationship_name
        return get_column(model, column_name)

    def _apply_offset(self, stmt: Select) -> Select:
        """
        Добавляет запросу stmt смещение

        :param stmt: запрос
        """
        if self._offset is not None:
            stmt = stmt.offset(self._offset)
        return stmt

    def _apply_limit(self, stmt: Select) -> Select:
        """
        Добавляет запросу stmt ограничение на количество

        :param stmt: запрос
        """
        if self._limit is not None:
            stmt = stmt.limit(self._limit)
        return stmt

    def _apply_where(self, stmt: Select) -> Select:
        """
        Добавляет запросу statement условия фильтрации

        :param stmt: запрос
        """
        return stmt.where(*self._where.values())

    def _apply_order_by(self, stmt: Select) -> Select:
        """
        Добавляет запросу сортировку

        :param stmt: запрос
        """
        return stmt.order_by(*self._order_by.values())

    def _apply_joins(self, stmt: Select, model_cls=None, joins: dict = None) -> Select:
        # todo: проjoinить одну таблицу несколько раз не получится
        model_cls = model_cls or self._model_cls
        joins = self._joins if joins is None else joins
        for join, value in joins.get("children", {}).items():
            isouter = value.get("isouter", False)
            relationship = self._get_relationship(model_cls, join)
            stmt = stmt.join(relationship.class_attribute, isouter=isouter)
            stmt = self._apply_joins(stmt, relationship.mapper.class_, value)
        return stmt

    def _get_model_pk(self) -> tuple[Column, ...]:
        """
        Возвращает кортеж столбцов, составляющих первичный ключ
        """
        return inspect(self._model_cls).primary_key

    def _validate_has_relationship(self, model_cls, relationship_name: str) -> None:
        """
        Валидирует, что модель model_cls имеет связь relationship_name

        :param model_cls: класс модели SQLAlchemy
        :param relationship_name: название связи
        """
        if relationship_name not in inspect(model_cls).relationships:
            raise RelationshipNotFoundError(model_cls, relationship_name)

    def _get_relationship(self, model_cls, relationship_name: str, raise_: bool = False) -> Relationship | None:
        """
        Возвращает связь (relationship) по ее названию relationship_name

        :param model_cls: класс модели SQLAlchemy
        :param relationship_name: название связи
        """
        relationship = inspect(model_cls).relationships.get(relationship_name)
        if not relationship and raise_:
            raise RelationshipNotFoundError(model_cls, relationship_name)
        return relationship
