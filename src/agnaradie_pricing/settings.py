"""Application settings and YAML config loading."""

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ModuleNotFoundError:
    from pydantic import BaseModel

    # Load .env manually when pydantic-settings isn't available.
    # Walk up from this file to find the project root .env.
    try:
        from dotenv import load_dotenv as _load_dotenv
        _dotenv_path = Path(__file__).parent.parent.parent / ".env"
        _load_dotenv(dotenv_path=_dotenv_path, override=False)
    except ModuleNotFoundError:
        pass

    class BaseSettings(BaseModel):
        def __init__(self, **data: Any) -> None:
            data.setdefault("database_url", os.getenv("DATABASE_URL"))
            data.setdefault("anthropic_api_key", os.getenv("ANTHROPIC_API_KEY"))
            data.setdefault("openai_api_key", os.getenv("OPENAI_API_KEY") or None)
            data.setdefault("openai_model", os.getenv("OPENAI_MODEL", "gpt-5-nano"))
            data.setdefault("log_level", os.getenv("LOG_LEVEL", "INFO"))
            data.setdefault("alert_webhook_url", os.getenv("ALERT_WEBHOOK_URL") or None)
            super().__init__(**data)

    SettingsConfigDict = dict


_PROJECT_ROOT = Path(__file__).parent.parent.parent


class Settings(BaseSettings):
    database_url: str = Field(default="postgresql+psycopg://user:pass@localhost:5432/agnaradie")
    anthropic_api_key: str | None = Field(default=None)
    openai_api_key: str | None = Field(default=None)
    openai_model: str = Field(default="gpt-5-nano")
    log_level: str = Field(default="INFO")
    alert_webhook_url: str | None = Field(default=None)

    model_config = SettingsConfigDict(env_file=str(_PROJECT_ROOT / ".env"), extra="ignore")


def load_competitors(path: Path = Path("config/competitors.yaml")) -> list[dict[str, Any]]:
    data = _load_yaml(path)
    competitors = data.get("competitors", [])
    if not isinstance(competitors, list):
        raise ValueError("config/competitors.yaml must define a competitors list")
    return competitors


def own_store_ids(path: Path = Path("config/competitors.yaml")) -> frozenset[str]:
    """Return the set of competitor IDs that are owned by AG (not true competitors)."""
    return frozenset(
        c["id"] for c in load_competitors(path) if c.get("own_store", False)
    )


def load_playbooks(path: Path = Path("config/playbooks.yaml")) -> dict[str, dict[str, float]]:
    data = _load_yaml(path)
    if not isinstance(data, dict):
        raise ValueError("config/playbooks.yaml must define a mapping")
    return data


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return data
