from pydantic import BaseModel


class KeyCreateBody(BaseModel):
    owner_id: str
    owner_type: str = "actor"
    tenant_id: str | None = None
    preset_names: list[str] | None = None
    label: str = ""
    expires_in_days: int | None = None


class KeyCreatedResponse(BaseModel):
    raw_key: str
    key_id: str
    owner_id: str
    label: str = ""
    expires_at: str | None = None


class KeyInfoResponse(BaseModel):
    key_id: str
    owner_id: str
    owner_type: str = "actor"
    label: str = ""
    is_active: bool = True
    expires_at: str | None = None
    created_at: str | None = None
    last_used_at: str | None = None
