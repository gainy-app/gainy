import datetime

from trading.models import KycStatus
from trading.drivewealth.models import DriveWealthKycStatus
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository


def test_sync_kyc(monkeypatch):
    user_ref_id = "user_ref_id"
    document_data = "document_data"
    kyc_data = {
        "kyc": {
            "status": {
                "name": "KYC_APPROVED"
            },
            "updated": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")
        }
    }

    drivewealth_repository = DriveWealthRepository(None)

    def mock_upsert_kyc_document(kyc_document_id, _data):
        assert _data == document_data

    monkeypatch.setattr(drivewealth_repository, "upsert_kyc_document",
                        mock_upsert_kyc_document)

    api = DriveWealthApi(drivewealth_repository)

    def mock_get_kyc_status(_user_ref_id):
        assert _user_ref_id == user_ref_id
        return DriveWealthKycStatus(kyc_data)

    def mock_get_user_documents(_user_ref_id):
        assert _user_ref_id == user_ref_id
        return [document_data]

    monkeypatch.setattr(api, "get_kyc_status", mock_get_kyc_status)
    monkeypatch.setattr(api, "get_user_documents", mock_get_user_documents)

    service = DriveWealthProvider(drivewealth_repository, api, None)
    kyc_status = service.sync_kyc(user_ref_id)

    assert kyc_status.status == KycStatus.APPROVED
