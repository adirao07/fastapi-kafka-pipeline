from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    DATABASE_URL: str = (
        "postgresql://pipeline_user:pipeline_password@postgres:5432/pipeline_db"
    )
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:29092"
    KAFKA_TOPIC: str = "events_stream"
    REDIS_URL: str = "redis://redis:6379/0"


settings = Settings()
