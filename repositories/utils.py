from sqlalchemy import inspect, Column
from sqlalchemy.ext.asyncio import AsyncSession

from repositories.exceptions import ColumnNotFoundError
from repositories.types import Model


def validate_has_columns(model_cls, *args: str) -> None:
    """
    Валидирует, что модель model имеет столбец column_name

    :param model_cls: класс модели SQLAlchemy
    :param args: название столбцов
    """
    columns = inspect(model_cls).columns
    for col in args:
        if col not in columns:
            raise ColumnNotFoundError(model_cls, col)


def get_column(model_cls, column_name: str) -> Column:
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
