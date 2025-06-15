from models.users import User
from repositories.base import BaseRepository
from repositories.queryset import QuerySet


class UsersRepository(BaseRepository):
    model_cls = User

    @property
    def active(self) -> QuerySet:
        return self.objects.filter(is_active=True)

    def user_restricted(self, user_id: int) -> QuerySet:
        return self.objects.filter(created_by_id=user_id)
