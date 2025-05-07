from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from dependencies import get_session
from repositories.users import UsersRepository
from schemas import CreateUserSchema, UserSchema

app = FastAPI()


@app.post("/user", response_model=UserSchema, status_code=201)
async def create_user(data: CreateUserSchema, session: AsyncSession = Depends(get_session)):
    async with session.begin():
        repository = UsersRepository(session)
        user = await repository.create(**data.model_dump())
    return user


@app.get("/first", response_model=UserSchema)
async def get_first(repository: UsersRepository = Depends()):
    return await repository.objects.filter(first_name__in=["Иван", "Петр"]).first()


@app.get("/ordering", response_model=list[UserSchema])
async def get_ordering(repository: UsersRepository = Depends()):
    users = (await repository.objects.order_by("-first_name", "-last_name").scalars()).all()
    return users


@app.get("/icontains", response_model=list[UserSchema])
async def get_users(repository: UsersRepository = Depends()):
    return await repository.objects.filter(type__status__id=1).select_related("type").all()


@app.get("/select-related", response_model=list[UserSchema])
async def get_select_related(repository: UsersRepository = Depends()):
    result = await repository.objects.select_related("type__status").scalars()
    users = result.all()
    return users
