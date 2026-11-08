from pathlib import Path
from typing import Annotated

import yaml
from pydantic import Field

from .models import Arguments, Engine


class EngineConfig(Arguments):
    name: Engine
    region: Annotated[int, Field(ge=0)]
    device: str = "mobile"


class RegionConfig(Arguments):
    name: str
    yandex: Annotated[int, Field(ge=0)] | None = None
    google: Annotated[int, Field(ge=0)] | None = None
    hints: list[str] = []
    expected_hosts: list[str] = []


class Config(Arguments):
    target_domain: str
    include_subdomains: bool = True
    depth: Annotated[int, Field(ge=10, le=100, multiple_of=10)] = 50
    engines: list[EngineConfig] = [EngineConfig(name="yandex", region=213)]
    regions: list[RegionConfig] = []
    phrases: list[str] = []
    csv: str | None = None
    column: str = "phrase"
    cache_ttl_hours: Annotated[float, Field(ge=0)] = 24
    max_attempts: Annotated[int, Field(ge=1)] = 1000
    deadline: Annotated[float, Field(gt=0)] = 180
    rate_max: Annotated[int, Field(ge=1)] = 5
    rate_window: Annotated[float, Field(gt=0)] = 1.5
    costs_per_1000: dict[Engine, Annotated[float, Field(ge=0)]] = {}


def load_config(path: Path) -> Config:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    cfg = Config.model_validate(data)
    if cfg.csv:
        cfg.csv = str((path.resolve().parent / cfg.csv).resolve())
    return cfg
