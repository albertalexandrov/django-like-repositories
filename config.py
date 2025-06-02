from pydantic import Field, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class DatabaseSettings(BaseModel):
    drivername: str = "postgresql+asyncpg"
    host: str = "change-me"
    port: int = 5432
    username: str = "change-me"
    password: str = "change-me"
    name: str = "change-me"

    @property
    def dsn(self):
        return URL.create(
            drivername=self.drivername,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.name,
        )


class Settings(BaseSettings):
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)

    model_config = SettingsConfigDict(
        env_prefix="DJANGO_LIKE_REPOSITORIES__", env_nested_delimiter="__"
    )


settings = Settings(_env_file=".env")
