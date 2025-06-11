class QueryBuilder:
    def __init__(self, model_cls):
        self._model_cls = model_cls
        self._wheres = {}

    def add_wheres(self, wheres):
        pass
