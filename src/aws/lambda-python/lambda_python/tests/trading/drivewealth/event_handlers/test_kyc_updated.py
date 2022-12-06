from gainy.tests.mocks.repository_mocks import mock_persist
from gainy.trading.drivewealth import DriveWealthRepository
from trading.drivewealth.event_handlers.kyc_updated import KycUpdatedEventHandler
from trading.drivewealth.provider import DriveWealthProvider
from trading.models import ProfileKycStatus, KycStatus


def test(monkeypatch):
    user_id = "user_id"
    profile_id = 1

    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_get_profile_id_by_user_id(_user_id):
        assert _user_id == user_id
        return profile_id

    monkeypatch.setattr(provider, 'get_profile_id_by_user_id',
                        mock_get_profile_id_by_user_id)

    event_handler = KycUpdatedEventHandler(repository, provider)

    message = {
        "userID": user_id,
        "current": {
            "status": "KYC_INFO_REQUIRED",
            "statusMessage":
            "User\u2019s PII does not match. Please check kyc errors for details and resubmit the information",
            "details": ["SSN_NOT_MATCH"]
        },
        "previous": {
            "status": "KYC_PROCESSING",
            "statusMessage": "User is sent for KYC",
            "details": ["SSN_NOT_MATCH"]
        }
    }
    event_handler.handle(message)

    assert ProfileKycStatus in persisted_objects
    entity: ProfileKycStatus = persisted_objects[ProfileKycStatus][0]
    assert entity.profile_id == profile_id
    assert entity.status == KycStatus.INFO_REQUIRED
    assert entity.message == message["current"]["statusMessage"]
    assert entity.error_messages == [
        "No match found for Social Security Number"
    ]
