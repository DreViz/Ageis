"""Wire format for the synthetic notification service."""
import hashlib
import json
from pydantic import BaseModel, ConfigDict, Field


class NoticeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    org_id: str = Field(min_length=1, max_length=128)
    recipient: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=1000)
    action_id: str = Field(min_length=1, max_length=128)


def request_hash(body: NoticeRequest) -> str:
    value = {"operation": "notice", **body.model_dump()}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
