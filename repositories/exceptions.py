class ImproperlyConfiguredFilterError(Exception):
    def __init__(self, filter_name: str):
        error = (
            f"По фильтру `{filter_name}` не удалось определить, по какому столбцу необходимо выполнить фильтрацию. "
            f"Убедитесь, что фильтр содержит одно поле для фильтрации"
        )
        super().__init__(error)
