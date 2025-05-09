from pydantic import BaseModel


class UserSchema(BaseModel):
    class Type(BaseModel):
        id: int
        code: str
        description: str
    id: int
    first_name: str
    last_name: str
    type: Type


class CreateUserSchema(BaseModel):
    first_name: str
    last_name: str
