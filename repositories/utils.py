from sqlalchemy import inspect, Column

from repositories.exceptions import ColumnNotFoundError


def validate_has_columns(model_cls, *cols: str) -> None:
    """
    Валидирует, что модель model имеет столбец column_name

    :param model_cls: класс модели SQLAlchemy
    :param cols: название столбцов
    """
    columns = inspect(model_cls).columns
    for col in cols:
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
