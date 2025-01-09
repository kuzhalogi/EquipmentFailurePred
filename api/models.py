from pydantic import BaseModel


class FetchPred(BaseModel):
    from_datetime: str
    to_datetime: str
    source: str


class ToPred(BaseModel):
    source: str
    df: str


