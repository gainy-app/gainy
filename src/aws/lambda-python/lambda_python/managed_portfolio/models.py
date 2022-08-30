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
