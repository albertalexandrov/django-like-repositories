from fastapi.params import Depends
from sqlalchemy import select, delete
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

    @property
    def objects(self) -> QuerySet:
        return QuerySet(self.model, self._session)

    async def all(self):
        stmt = select(self.model)
        result = await self._session.scalars(stmt)
        return result.all()

    async def first(self):
        stmt = select(self.model).limit(1)
        return await self._session.scalar(stmt)

    async def delete(self):
        stmt = delete(self.model)
        await self._session.execute(stmt)
