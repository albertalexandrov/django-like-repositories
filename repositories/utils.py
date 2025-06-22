from typing import Type, List

from sqlalchemy import inspect, Column, ColumnCollection
from sqlalchemy.orm import Relationship
from sqlalchemy.ext.asyncio import AsyncSession

from repositories.exceptions import ColumnNotFoundError
from repositories.types import Model


def validate_has_columns(model_cls: Type[Model], *args: str) -> None:
    columns = inspect(model_cls).columns
    for col in args:
        if col not in columns:
            raise ColumnNotFoundError(model_cls, col)


def get_column(model_cls: Type[Model], column_name: str) -> Column:
    column = inspect(model_cls).columns.get(column_name)
    if column is not None:
        return column
    raise ColumnNotFoundError(model_cls, column_name)


def get_columns(model_cls: Type[Model]) -> ColumnCollection:
    return inspect(model_cls).columns


async def flush_or_commit(*objs: Model, session: AsyncSession, flush: bool, commit: bool) -> None:
    if flush and not commit:
        await session.flush(objs)
    elif commit:
        await session.commit()


def get_relationship(model_cls: Type[Model], relationship_name: str) -> Relationship | None:
    relationship = inspect(model_cls).relationships.get(relationship_name)
    if relationship is not None:
        return relationship
    raise ValueError(f"В модели {model_cls.__name__} отсутствует связь `{relationship_name}`")


def get_pk(model_cls: Type[Model]) -> Column:
    pk = inspect(model_cls).primary_key
    if len(pk) == 1:
        return pk[0]
    raise ValueError(
        f"Модель {model_cls.__name__} имеет составной первичный ключ. "
        f"Работа с составными первичными ключами невозможна"
    )


def get_relationships(model_cls: Type[Model]) -> Relationship:
    return inspect(model_cls).relationships


def get_annotations(model_cls: Type[Model]) -> dict:
    return model_cls.__dict__["__annotations__"]
