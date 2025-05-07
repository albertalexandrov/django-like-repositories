from fastapi.params import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dependencies import get_session
from repositories.queryset import QuerySet


class BaseRepository:
    model = None

    def __init__(self, session: AsyncSession = Depends(get_session)):
        self._session = session

    async def create(self, **values):
        instance = self.model(**values)
        self._session.add(instance)
        return instance

    async def all(self):
        stmt = select(self.model)
        result = await self._session.scalars(stmt)
        return result.all()

    @property
    def objects(self):
        return QuerySet(self.model, self._session)
