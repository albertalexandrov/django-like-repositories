from models import Section
from repositories.base import BaseRepository


class SectionRepository(BaseRepository):
    model = Section

    @property
    def published(self):
        return self.objects.filter(status__code='published')
