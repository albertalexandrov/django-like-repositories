import random

from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from dependencies import get_session
from repositories.help import SectionRepository
from repositories.users import UsersRepository
from schemas import CreateUserSchema, UserSchema
from models.help import Section

app = FastAPI()


@app.post("/user", response_model=UserSchema, status_code=201)
async def create_user(
    data: CreateUserSchema, session: AsyncSession = Depends(get_session)
):
    async with session.begin():
        repository = UsersRepository(session)
        user = await repository.create(**data.model_dump())
    return user


@app.get("/first")
async def get_first(repository: UsersRepository = Depends()):
    return (
        await repository.objects.filter(id=1)
        .options("type__status", "type__change_logs", "documents")
        .first()
    )


@app.get("/ordering", response_model=list[UserSchema])
async def get_ordering(repository: UsersRepository = Depends()):
    return (
        await repository.objects.order_by("-first_name", "-last_name")
        .options("type")
        .all()
    )


@app.get("/icontains", response_model=list[UserSchema])
async def get_users(repository: UsersRepository = Depends()):
    return await repository.objects.filter(type__code="sh").options("type").all()


@app.get("/select-related", response_model=list[UserSchema])
async def get_select_related(repository: UsersRepository = Depends()):
    return await repository.objects.filter(type__id=1).options("type__status").all()


@app.get("/order-by", response_model=list[UserSchema])
async def get_select_related(repository: UsersRepository = Depends()):
    return await repository.objects.options("type").order_by("type__description").all()


@app.get("/active-only", response_model=list[UserSchema])
async def get_active_only(repository: UsersRepository = Depends()):
    return await repository.active.options("type").order_by("id").all()


@app.get("/created-by", response_model=list[UserSchema])
async def get_created_by(repository: UsersRepository = Depends()):
    return await repository.user_restricted(1).options("type").all()


@app.get("/test")
async def test(session: AsyncSession = Depends(get_session), repository: SectionRepository = Depends()):
    queryset = (
        repository
        .objects
    )
    status_id = random.choice([1, 2])
    print(status_id)
    return await queryset.in_bulk(id_list=[1], field_name="status_id")


@app.get("/test-on-session")
async def test_on_session(session: AsyncSession = Depends(get_session)):
    stmt = select(Section)
    result = await session.scalars(stmt)
    result.one()