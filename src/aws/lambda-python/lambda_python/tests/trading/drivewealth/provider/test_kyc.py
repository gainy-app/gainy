import datetime

from gainy.tests.mocks.repository_mocks import mock_persist, mock_noop
from gainy.utils import DATETIME_ISO8601_FORMAT_TZ
from trading.models import KycStatus, ProfileKycStatus
from trading.drivewealth.models import DriveWealthKycStatus
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository


def test_sync_kyc(monkeypatch):
    user_ref_id = "user_ref_id"
    profile_id = 1
    document_data = "document_data"
    kyc_data = {
        "kyc": {
            "status": {
                "name": "KYC_APPROVED"
            },
            "updated":
            datetime.datetime.now().strftime(DATETIME_ISO8601_FORMAT_TZ)
        }
    }

    drivewealth_repository = DriveWealthRepository(None)

    def mock_upsert_kyc_document(kyc_document_id, _data):
        assert _data == document_data

    monkeypatch.setattr(drivewealth_repository, "upsert_kyc_document",
                        mock_upsert_kyc_document)
    persisted_objects = {}
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))

    api = DriveWealthApi(drivewealth_repository)

    def mock_get_kyc_status(_user_ref_id):
        assert _user_ref_id == user_ref_id
        return DriveWealthKycStatus(kyc_data)

    def mock_get_user_documents(_user_ref_id):
        assert _user_ref_id == user_ref_id
        return [document_data]

    monkeypatch.setattr(api, "get_kyc_status", mock_get_kyc_status)
    monkeypatch.setattr(api, "get_user_documents", mock_get_user_documents)

    service = DriveWealthProvider(drivewealth_repository, api, None, None,
                                  None)

    def mock_get_profile_id_by_user_id(user_id):
        assert user_id == user_ref_id
        return profile_id

    monkeypatch.setattr(service, "get_profile_id_by_user_id",
                        mock_get_profile_id_by_user_id)
    monkeypatch.setattr(service, "sync_user", mock_noop)
    kyc_status = service.sync_kyc(user_ref_id)

    assert kyc_status.status == KycStatus.APPROVED
    assert kyc_status.profile_id == profile_id
    assert ProfileKycStatus in persisted_objects
    assert kyc_status in persisted_objects[ProfileKycStatus]
