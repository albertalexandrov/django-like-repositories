from typing import Self

from sqlalchemy.orm import DeclarativeBase

from db import metadata


class Base(DeclarativeBase):
    metadata = metadata

    def update(self, **values) -> Self:
        for key, value in values.items():
            setattr(self, key, value)
        return self
