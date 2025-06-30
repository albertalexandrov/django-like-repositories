from itertools import islice
from typing import Generic, Any, Type, Self

from fastapi.params import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from dependencies import get_session
from repositories.queryset import QuerySet
from repositories.types import Model


class BaseRepository(Generic[Model]):
    model_cls: Type[Model] = None

    def __init__(self, session: AsyncSession = Depends(get_session)):
        if not self.model_cls:
            raise ValueError("Не задана модель в атрибуте `model_cls`")
        self._session = session
        self._flush = None
        self._commit = None

    def _clone(self) -> Self:
        clone = self.__class__(session=self._session)
        clone._flush = self._flush
        clone._commit = self._commit
        return clone

    def flush(self, flush: bool = True, /) -> Self:
        clone = self._clone()
        clone._flush = flush
        return clone

    def commit(self, commit: bool = True, /) -> Self:
        clone = self._clone()
        clone._commit = commit
        return clone

    async def _flush_commit_reset(self, *objs: Model) -> None:
        if self._flush and not self._commit and objs:
            await self._session.flush(objs)
        elif self._commit:
            await self._session.commit()
        self._flush = None
        self._commit = None

    async def create(self, **kw: dict[str:Any]) -> Model:
        obj = self.model_cls(**kw)
        self._session.add(obj)
        await self._flush_commit_reset(obj)
        return obj

    async def bulk_create(self, values: list[dict], batch_size: int = None) -> list[Model]:
        if batch_size is not None and (not isinstance(batch_size, int) or batch_size <= 0):
            raise ValueError("batch_size должен быть целым положительным числом")
        objs = []
        if batch_size:
            it = iter(values)
            while batch := list(islice(it, batch_size)):
                batch_objs = [self.model_cls(**item) for item in batch]
                await self._flush_commit_reset(*batch_objs)
                objs.extend(batch_objs)
        else:
            for item in values:
                obj = self.model_cls(**item)
                objs.append(obj)
            await self._flush_commit_reset(objs)
        return objs

    async def get_by_pk(self, pk: Any) -> Model:
        return await self._session.get(self.model_cls, pk)

    @property
    def objects(self) -> QuerySet:
        return QuerySet(self.model_cls, self._session)
