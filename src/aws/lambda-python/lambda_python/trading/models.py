import datetime
import json

import enum

from gainy.data_access.models import BaseModel, classproperty
from gainy.exceptions import BadRequestException


class KycDocumentType(str, enum.Enum):
    DRIVER_LICENSE = "DRIVER_LICENSE"
    PASSPORT = "PASSPORT"
    NATIONAL_ID_CARD = "NATIONAL_ID_CARD"
    VOTER_ID = "VOTER_ID"
    WORK_PERMIT = "WORK_PERMIT"
    VISA = "VISA"
    RESIDENCE_PERMIT = "RESIDENCE_PERMIT"


class KycDocumentSide(str, enum.Enum):
    FRONT = "FRONT"
    BACK = "BACK"


class KycDocument(BaseModel):
    id = None
    profile_id = None
    uploaded_file_id = None
    type: KycDocumentType = None
    side: KycDocumentSide = None
    content_type = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if row and row["type"]:
            self.type = KycDocumentType(row["type"])

        if row and row["side"]:
            self.side = KycDocumentSide(row["side"])
        return self

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "kyc_documents"

    def validate(self):
        if self.type in [KycDocumentType.PASSPORT, KycDocumentType.VISA
                         ] and self.side == KycDocumentSide.BACK:
            raise BadRequestException(
                f"Document of type {self.type} can't have BACK side")
