import datetime

from actions import VerificationVerifyCode
from common.context_container import ContextContainer
from gainy.data_access.repository import Repository
from gainy.tests.mocks.repository_mocks import mock_find, mock_record_calls
from verification.models import VerificationCode
from verification.service import VerificationService


def test(monkeypatch):
    user_input = "user_input"
    verification_code_id = 2
    created_at = datetime.datetime.now(tz=datetime.timezone.utc)

    verification_code = VerificationCode()
    monkeypatch.setattr(verification_code, "created_at", created_at)

    verification_service = VerificationService(None, None)
    verify_code_calls = []
    monkeypatch.setattr(verification_service, "verify_code",
                        mock_record_calls(verify_code_calls))

    repository = Repository(None)
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([(VerificationCode, {
            "id": verification_code_id
        }, verification_code)]))

    context_container = ContextContainer()
    monkeypatch.setattr(context_container, "get_repository",
                        lambda: repository)
    monkeypatch.setattr(context_container, "verification_service",
                        verification_service)

    input_params = {
        "verification_code_id": verification_code_id,
        "user_input": user_input,
    }
    action = VerificationVerifyCode()

    assert {
        "ok": True,
    } == action.apply(input_params, context_container)
    assert (verification_code,
            user_input) in [args for args, kwargs in verify_code_calls]
