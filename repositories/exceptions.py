class ColumnNotFoundError(Exception):
    def __init__(self, model, column_name: str):
        error = (
            f"Столбец `{column_name}` не найден в модели {model.__name__}"
        )
        super().__init__(error)
