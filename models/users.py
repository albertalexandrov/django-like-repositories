from sqlalchemy import ForeignKey, true
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.base import Base


class UserTypeStatus(Base):
    __tablename__ = 'user_status_types'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    user_types: Mapped[list["UserType"]] = relationship(back_populates="status")


class UserType(Base):
    __tablename__ = 'user_types'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    code: Mapped[str]
    description: Mapped[str]
    users: Mapped[list["User"]] = relationship(back_populates="type")
    status_id: Mapped[int | None] = mapped_column(ForeignKey('user_status_types.id'), nullable=True)
    status: Mapped["UserTypeStatus"] = relationship(back_populates="user_types")


class User(Base):
    __tablename__ = 'users'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    first_name: Mapped[str]
    last_name: Mapped[str]
    type_id: Mapped[int | None] = mapped_column(ForeignKey('user_types.id'))
    type: Mapped["UserType"] = relationship(back_populates="users")
    is_active: Mapped[bool] = mapped_column(default=True, server_default=true())
