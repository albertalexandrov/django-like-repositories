from sqlalchemy.orm import DeclarativeBase

from db import metadata


class Base(DeclarativeBase):
    metadata = metadata

    def update(self, **values):
        for key, value in values.items():
            setattr(self, key, value)
        return self
