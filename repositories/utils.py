from typing import Type

from sqlalchemy import inspect, Column
from sqlalchemy.orm import Relationship
from sqlalchemy.ext.asyncio import AsyncSession

from repositories.exceptions import ColumnNotFoundError
from repositories.types import Model


def validate_has_columns(model_cls: Type[Model], *args: str) -> None:
    """
    Валидирует, что модель model имеет столбец column_name

    :param model_cls: класс модели SQLAlchemy
    :param args: название столбцов
    """
    columns = inspect(model_cls).columns
    for col in args:
        if col not in columns:
            raise ColumnNotFoundError(model_cls, col)


def get_column(model_cls: Type[Model], column_name: str) -> Column:
    """
    Возвращает столбец по его названию column_name

    :param model_cls: класс модели SQLAlchemy
    :param column_name: название столбца
    """
    column = inspect(model_cls).columns.get(column_name)
    if column is not None:
        return column
    raise ColumnNotFoundError(model_cls, column_name)


async def flush_or_commit(*objs: Model, session: AsyncSession, flush: bool, commit: bool) -> None:
    """
    Выполняет flush или commit

    :param objs: экземпляры для flush
    :param session: сессия
    :param flush: признак необходимости выполнить flush
    :param commit: признак необходимости выполнить сommit
    """
    if flush and not commit:
        await session.flush(objs)
    elif commit:
        await session.commit()


def get_relationship(model_cls: Type[Model], relationship_name: str, raise_: bool = False) -> Relationship | None:
    """
    Возвращает связь (relationship) по ее названию relationship_name

    :param model_cls: класс модели SQLAlchemy
    :param relationship_name: название связи
    :param raise_: признак необходимости рейзить исключение при отсутствии связи
    """
    relationship = inspect(model_cls).relationships.get(relationship_name)
    if not relationship and raise_:
        raise ValueError(f"В модели {model_cls.__name__} отсутствует связь `{relationship_name}`")
    return relationship


def get_model_pk(model_cls: Type[Model]) -> tuple[Column, ...]:
    """
    Возвращает кортеж столбцов, составляющих первичный ключ
    """
    return inspect(model_cls).primary_key
