import pytest

from actions import VerificationSendCode
from common.context_container import ContextContainer
from verification.models import VerificationCodeChannel, VerificationCode
from verification.service import VerificationService


def get_test_channel():
    return [
        VerificationCodeChannel.SMS.name, VerificationCodeChannel.EMAIL.name
    ]


@pytest.mark.parametrize("channel", get_test_channel())
def test(monkeypatch, channel):
    profile_id = 1
    verification_code_id = 2
    address = "address"

    verification_code = VerificationCode()
    monkeypatch.setattr(verification_code, "id", verification_code_id)

    verification_service = VerificationService(None, None)

    def mock_send_code(**kwargs):
        assert kwargs["profile_id"] == profile_id
        assert kwargs["channel"] == VerificationCodeChannel(channel)
        assert kwargs["address"] == address
        return verification_code

    monkeypatch.setattr(verification_service, "send_code", mock_send_code)

    context_container = ContextContainer()
    monkeypatch.setattr(context_container, "verification_service",
                        verification_service)

    input_params = {
        "profile_id": profile_id,
        "channel": channel,
        "address": address,
    }

    action = VerificationSendCode()

    assert {
        "verification_code_id": verification_code_id,
    } == action.apply(input_params, context_container)
