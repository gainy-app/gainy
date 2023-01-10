import datetime

import enum

from gainy.data_access.models import BaseModel, classproperty


class VerificationCodeChannel(str, enum.Enum):
    EMAIL = "EMAIL"
    SMS = "SMS"


class VerificationCode(BaseModel):
    id: str = None
    profile_id: int = None
    channel: VerificationCodeChannel = None
    address: str = None
    failed_at: datetime.datetime = None
    verified_at: datetime.datetime = None
    created_at: datetime.datetime = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if row and row["channel"]:
            self.channel = VerificationCodeChannel(row["channel"])

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "channel":
            self.channel.value if self.channel else None,
        }

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "verification_codes"
