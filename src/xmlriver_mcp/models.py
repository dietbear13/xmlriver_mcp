from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

Query = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Engine = Literal["yandex", "google"]
DEFAULT_REGION = {"yandex": 213, "google": 1011969}


class Arguments(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SerpArgs(Arguments):
    query: Query
    engine: Engine = "yandex"
    region: Annotated[int, Field(ge=0)] | None = None
    device: Literal["desktop", "mobile", "tablet"] = "mobile"
    pages: Annotated[int, Field(ge=1, le=10)] = 1
    target_domain: Query | None = None
    include_subdomains: bool = True


class WordstatArgs(Arguments):
    query: Query
    regions: Annotated[int, Field(ge=0)] | None = 213
    device: Literal["desktop", "phone", "tablet"] | None = None


class TopArgs(WordstatArgs):
    limit: Annotated[int, Field(ge=1, le=1000)] = 50
    min_frequency: Annotated[int, Field(ge=0)] = 0
    include_associations: bool = False
    drop_self: bool = True


class HistoryArgs(WordstatArgs):
    period: Literal["month", "week", "day"] = "month"
    start: str | None = None
    end: str | None = None

    @model_validator(mode="after")
    def dates(self):
        parsed = []
        for value in (self.start, self.end):
            if value is not None:
                date = datetime.strptime(value, "%d.%m.%Y")
                if date.strftime("%d.%m.%Y") != value:
                    raise ValueError("Expected dd.mm.yyyy")
                parsed.append(date)
        if len(parsed) == 2 and parsed[0] > parsed[1]:
            raise ValueError("start must precede end")
        return self


INPUTS: dict[str, type[Arguments]] = {
    "get_balance": Arguments,
    "serp": SerpArgs,
    "wordstat_top": TopArgs,
    "wordstat_history": HistoryArgs,
}


class ObjectResult(BaseModel):
    """Provider history stays an opaque JSON object; no invented series schema."""

    model_config = ConfigDict(extra="allow")


class BalanceResult(ObjectResult):
    balance: float
    cost_google_per_1000: float | None
    cost_yandex_per_1000: float | None


class TopResult(ObjectResult):
    query: str
    regions: int | None
    returned: int
    total_popular: int
    total_associations: int
    phrases: list[dict]


class SerpResult(ObjectResult):
    query: str
    engine: Engine
    region: int
    device: str
    results_count: int
    results: list[dict]


OUTPUTS: dict[str, type[ObjectResult]] = {
    "get_balance": BalanceResult,
    "serp": SerpResult,
    "wordstat_top": TopResult,
    "wordstat_history": ObjectResult,
}
