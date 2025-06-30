from fastapi import FastAPI, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import with_loader_criteria, selectinload, joinedload, contains_eager, aliased

from dependencies import get_session
from models import Subsection
from models.help import Section
from repositories.help import SectionRepository
from repositories.users import UsersRepository
from schemas import CreateUserSchema, UserSchema

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
async def test(
    session: AsyncSession = Depends(get_session),
    repository: SectionRepository = Depends(),
):
    queryset = (
        repository
        .objects
        # .filter(subsections__status__id__in=[1, 2])
        # .filter(subsections__status__id=2)
        # .filter(subsections__id=None)
        # .filter(status__code=)
        # .filter(status_id=1)
        # .values_list('id', 'name')
        # .order_by('id')
        # .order_by('status__code')
        # .order_by('-subsections__status__code')
        # .outerjoin("subsections__status")
        # .outerjoin("subsections")
        # .limit(2)
        # .options("subsections")
        # .options("subsections__status")
        # .options("subsections__status")
        # .returning('id', 'name')
        # .execution_options(populate_existing=True)
    )

    res = await repository.create(name="Section", status_id=1)
    print(res.id)
    return res


@app.get("/test-on-session")
async def test_on_session(session: AsyncSession = Depends(get_session)):
    # isouter = True
    # stmt = (
    #     select(Section).distinct()
    #     .join(Section.subsections, isouter=isouter)
    #     .options(
    #         joinedload(Section.subsections, innerjoin=not isouter),
    #         # with_loader_criteria(Subsection, Subsection.status_id == 1)
    #     )
    #     .limit(2)
    #     .where(Subsection.id == 1)
    # )
    subquery = select(Section).distinct().limit(2)
    SectionAlias = aliased(Section, subquery.subquery())
    stmt = select(SectionAlias)
    stmt = stmt.join(SectionAlias.subsections)
    stmt = stmt.options(contains_eager(SectionAlias.subsections))
    result = await session.scalars(stmt)
    print(result.all())
    # for section in result.unique().all():
    #     print(section.id, len(section.subsections))
