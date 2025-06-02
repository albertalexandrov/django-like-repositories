from typing import TypeVar, Generic, Any

from fastapi.params import Depends
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from dependencies import get_session
from repositories.queryset import QuerySet

Model = TypeVar("Model")


class BaseRepository(Generic[Model]):
    model: Model = None

    def __init__(self, session: AsyncSession = Depends(get_session)):
        self._session = session

    async def create(self, values: dict, flush: bool = True, commit: bool = False) -> Model:
        instance = self.model(**values)
        self._session.add(instance)
        if flush and not commit:
            await self._session.flush(instance)
        elif commit:
            await self._session.commit()
        return instance

    async def bulk_create(self, values: list[dict], flush: bool = True, commit: bool = False) -> list[Model]:
        objs = []
        for item in values:
            obj = self.model(**item)
            objs.append(obj)
        if flush and not commit:
            await self._session.flush(*objs)
        elif commit:
            await self._session.commit()
        return objs

    # методы с ограниченной функциональностью

    async def all(self) -> list[Model]:
        stmt = select(self.model)
        result = await self._session.scalars(stmt)
        return result.all()  # noqa

    async def get_by_pk(self, pk: Any) -> Model:
        return await self._session.get(self.model, pk)

    async def delete(self) -> None:
        stmt = delete(self.model)
        await self._session.execute(stmt)

    async def first(self) -> Model | None:
        stmt = select(self.model).limit(1)
        return await self._session.scalar(stmt)

    # возможно какие то другие методы с ограниченной фунциональностью (получение списка, напр.) - накидывайте

    @property
    def objects(self) -> QuerySet:
        return QuerySet(self.model, self._session)
