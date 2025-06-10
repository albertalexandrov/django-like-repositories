from models import Section, PublicationStatus
from repositories.base import BaseRepository


class SectionRepository(BaseRepository):
    model = Section

    @property
    def published(self):
        return self.objects.filter(status__code='published')


class PublicationStatusRepository(BaseRepository):
    model = PublicationStatus

    async def get_PUBLISHED_status(self) -> PublicationStatus:  # noqa
        return await self.objects.filter(code='published').get_one_or_raise()

    async def get_UNPUBLISHED_status(self) -> PublicationStatus:  # noqa
        return await self.objects.filter(code='unpublished').get_one_or_raise()
