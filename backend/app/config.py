from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "Intelligent Data Cleaner"
    VERSION: str = "0.1.0"
    MAX_UPLOAD_SIZE: int = 100 * 1024 * 1024  # 100MB
    
    class Config:
        env_file = ".env"


settings = Settings()