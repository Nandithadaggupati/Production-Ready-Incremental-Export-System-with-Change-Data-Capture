import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/mydatabase")
    PORT: int = int(os.environ.get("PORT", "8080"))
    HOST: str = os.environ.get("HOST", "0.0.0.0")
    OUTPUT_DIR: str = os.environ.get("OUTPUT_DIR", "/app/output")

    class Config:
        env_file = ".env"

settings = Settings()
