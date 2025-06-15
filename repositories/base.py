from itertools import islice
from typing import Generic, Any, Type

from fastapi.params import Depends
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from dependencies import get_session
from repositories.queryset import QuerySet
from repositories.types import Model
from repositories.utils import flush_or_commit


class BaseRepository(Generic[Model]):
    model_cls: Type[Model] = None

    def __init__(self, session: AsyncSession = Depends(get_session)):
        if not self.model_cls:
            raise ValueError("Не задана модель в атрибуте `model_cls`")
        self._session = session

    async def create(self, values: dict[str:Any], flush: bool = True, commit: bool = False) -> Model:
        obj = self.model_cls(**values)
        self._session.add(obj)
        await flush_or_commit(obj, session=self._session, flush=flush, commit=commit)
        return obj

    async def bulk_create(
        self, values: list[dict], batch_size: int = None, flush: bool = True, commit: bool = False
    ) -> list[Model]:
        if batch_size is not None and (not isinstance(batch_size, int) or batch_size <= 0):
            raise ValueError("batch_size должен быть целым положительным числом")
        objs = []
        if batch_size:
            it = iter(values)
            while batch := list(islice(it, batch_size)):
                batch_objs = [self.model_cls(**item) for item in batch]
                await flush_or_commit(*objs, session=self._session, flush=flush, commit=commit)
                objs.extend(batch_objs)
        else:
            for item in values:
                obj = self.model_cls(**item)
                objs.append(obj)
            await flush_or_commit(*objs, session=self._session, flush=flush, commit=commit)
        return objs

    # методы с ограниченной функциональностью

    async def all(self) -> list[Model]:
        stmt = select(self.model_cls)
        result = await self._session.scalars(stmt)
        return result.all()  # noqa

    async def get_by_pk(self, pk: Any) -> Model:
        return await self._session.get(self.model_cls, pk)

    async def delete(self) -> None:
        stmt = delete(self.model_cls)
        await self._session.execute(stmt)

    async def first(self) -> Model | None:
        stmt = select(self.model_cls).limit(1)
        return await self._session.scalar(stmt)

    # возможно какие то другие методы с ограниченной фунциональностью (получение списка, напр.) - накидывайте

    @property
    def objects(self) -> QuerySet:
        return QuerySet(self.model_cls, self._session)
