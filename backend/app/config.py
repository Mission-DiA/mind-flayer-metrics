from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # GCP
    gcp_project: str = "kf-dev-ops-p001"
    bigquery_dataset: str = "billing"
    bigquery_table: str = "fact_cloud_costs"

    # Gemini
    gemini_api_key: str = ""
    gemini_model: str = "gemini-1.5-pro"

    # App
    allowed_origins: str = "http://localhost:3000"
    rate_limit_per_hour: int = 20
    log_level: str = "INFO"

    @property
    def bigquery_full_table(self) -> str:
        return f"{self.gcp_project}.{self.bigquery_dataset}.{self.bigquery_table}"

    @property
    def origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(",")]

    class Config:
        env_file = ".env"


settings = Settings()
