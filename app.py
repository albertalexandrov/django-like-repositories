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
    return await repository.objects.filter(first_name__in=["Иван", "Петр"]).options('type').first()


@app.get("/ordering", response_model=list[UserSchema])
async def get_ordering(repository: UsersRepository = Depends()):
    return await repository.objects.order_by("-first_name", "-last_name").options("type").all()


@app.get("/icontains", response_model=list[UserSchema])
async def get_users(repository: UsersRepository = Depends()):
    return await repository.objects.filter(type__code="sh").options("type").all()


@app.get("/select-related", response_model=list[UserSchema])
async def get_select_related(repository: UsersRepository = Depends()):
    return await repository.objects.filter(type__id=1).options("type__status").all()


@app.get("/order-by", response_model=list[UserSchema])
async def get_select_related(repository: UsersRepository = Depends()):
    return await repository.objects.options('type').order_by("type__description").all()


@app.get("/active-only", response_model=list[UserSchema])
async def get_active_only(repository: UsersRepository = Depends()):
    return await repository.active.options('type').order_by('id').all()
