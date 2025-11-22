from pydantic_settings import BaseSettings
from typing import Optional, Literal
import os

class Settings(BaseSettings):
    DB1_CONNECTION_STRING: str
    DB2_CONNECTION_STRING: str
    AI_PROVIDER: Literal["openai", "gemini"] = "openai"
    OPENAI_API_KEY: Optional[str] = None
    GEMINI_API_KEY: Optional[str] = None
    OPENAI_MODEL: str = "gpt-4o-2024-08-06"  
    GEMINI_MODEL: str = "gemini-2.5-flash"  
    BATCH_SIZE: int = 10000
    MAX_WORKERS: int = 10
    ENABLE_DASK: bool = True
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    ENABLE_METRICS: bool = True
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        # Allow extra fields or ignore them
        extra = "ignore"

settings = Settings()
