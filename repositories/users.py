from models.users import User
from repositories.base import BaseRepository


class UsersRepository(BaseRepository):
    model = User
