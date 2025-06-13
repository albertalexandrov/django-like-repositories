class ColumnNotFoundError(Exception):
    def __init__(self, model, column_name: str):
        error = (
            f"Столбец `{column_name}` не найден в модели {model.__name__}"
        )
        super().__init__(error)


class RelationshipNotFoundError(Exception):
    def __init__(self, model_cls, relationship_name: str):
        error = f"В модели {model_cls.__name__} отсутствует связь `{relationship_name}`"
        super().__init__(error)
