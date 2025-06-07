class ObjectNotFoundError(Exception):
    def __init__(self, error: str = "Объект не найден"):
        super().__init__(error)
