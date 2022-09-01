import enum


class KycStatus(str, enum.Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REVOKED = "REVOKED"
    CLOSED = "CLOSED"


class ProfileKycStatus:
    status = None

    def __init__(self, status: str):
        self.status = KycStatus(status)


class KycDocument:
    uploaded_file_id = None
    uploaded_file_id = None
    type = None
    side = None
    content_type = None

    def __init__(self, uploaded_file_id, type, side):
        self.uploaded_file_id = uploaded_file_id
        self.type = type
        self.side = side
