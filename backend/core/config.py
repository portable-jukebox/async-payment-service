from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    app_name: str = "Payment Service"
    database_url: str
    test_database_url: str | None = None
    api_key: str
    rabbitmq_url: str
    webhook_signing_secret: SecretStr | None = None
    outbox_cleanup_enabled: bool = False
    outbox_retention_days: int = Field(default=7, ge=1)
    outbox_cleanup_batch_size: int = Field(default=500, ge=1, le=10_000)
    outbox_cleanup_interval_seconds: int = Field(default=3600, ge=60)
    metrics_enabled: bool = True
    worker_metrics_port: int = Field(default=9001, ge=1024, le=65535)
    publisher_metrics_port: int = Field(default=9002, ge=1024, le=65535)

    def signing_key(self) -> bytes:
        key = self.webhook_signing_secret
        if key is None or len(key.get_secret_value().encode()) < 32:
            raise ValueError("WEBHOOK_SIGNING_SECRET must contain at least 32 bytes")
        return key.get_secret_value().encode()
