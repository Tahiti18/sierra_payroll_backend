from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    app_name: str = "Sierra Roofing Payroll Automation"
    debug: bool = False

    # Database
    database_url: str

    # Security
    secret_key: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30

    # WBS Format Configuration
    wbs_version: str = "B90216-00"
    wbs_format_rev: str = "2.1"
    client_unique_id: str = "055269"
    client_name: str = "Sierra Roofing and Solar Inc"

    class Config:
        env_file = ".env"

settings = Settings()
