from models.users import User
from repositories.base import BaseRepository


class UsersRepository(BaseRepository):
    model = User

    @property
    def active(self):
        return self.objects.filter(is_active=True)
