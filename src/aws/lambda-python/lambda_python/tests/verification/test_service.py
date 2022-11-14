import pytest

from gainy.data_access.repository import Repository
from gainy.tests.mocks.repository_mocks import mock_record_calls, mock_persist
from verification.client.email import EmailVerificationClient
from verification.client.sms import SmsVerificationClient
from verification.models import VerificationCode, VerificationCodeChannel
from verification.service import VerificationService
import verification.service as service_module


def get_test_channel():
    return [VerificationCodeChannel.SMS, VerificationCodeChannel.EMAIL]


def mock_supports(channel: VerificationCodeChannel):

    def mock(entity: VerificationCode):
        assert entity.channel == channel
        return True

    return mock


@pytest.mark.parametrize("channel", get_test_channel())
def test_send_code(monkeypatch, channel):
    profile_id = 1
    address = "address"

    if channel == VerificationCodeChannel.EMAIL:
        client = EmailVerificationClient(None)
    elif channel == VerificationCodeChannel.SMS:
        client = SmsVerificationClient(None)
    else:
        raise Exception(f'Unsupported channel {channel}')

    monkeypatch.setattr(client, "supports", mock_supports(channel))
    validate_address_calls = []
    monkeypatch.setattr(client, "validate_address",
                        mock_record_calls(validate_address_calls))
    send_calls = []
    monkeypatch.setattr(client, "send", mock_record_calls(send_calls))

    repository = Repository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    service = VerificationService(repository, [client])

    check_can_send_calls = []

    def mock_check_can_send(*args, **kwargs):
        mock_record_calls(check_can_send_calls)(*args, **kwargs)
        return True

    monkeypatch.setattr(service, "_check_can_send", mock_check_can_send)

    entity = service.send_code(profile_id, channel, address)

    assert entity.profile_id == profile_id
    assert entity.address == address
    assert entity.channel == channel
    assert entity in persisted_objects[VerificationCode]
    assert entity in [args[0] for args, kwargs in check_can_send_calls]
    assert entity in [args[0] for args, kwargs in validate_address_calls]
    assert entity in [args[0] for args, kwargs in send_calls]


@pytest.mark.parametrize("channel", get_test_channel())
def test_verify_code(monkeypatch, channel):
    user_input = "user_input"

    entity = VerificationCode()
    monkeypatch.setattr(entity, "channel", channel)

    if channel == VerificationCodeChannel.EMAIL:
        client = EmailVerificationClient(None)
    elif channel == VerificationCodeChannel.SMS:
        client = SmsVerificationClient(None)
    else:
        raise Exception(f'Unsupported channel {channel}')

    monkeypatch.setattr(client, "supports", mock_supports(channel))
    check_user_input_calls = []
    monkeypatch.setattr(client, "check_user_input",
                        mock_record_calls(check_user_input_calls))

    repository = Repository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    check_can_verify_calls = []
    monkeypatch.setattr(service_module, "_check_can_verify",
                        mock_record_calls(check_can_verify_calls))

    service = VerificationService(repository, [client])
    service.verify_code(entity, user_input)

    assert entity.verified_at is not None
    assert entity in persisted_objects[VerificationCode]
    assert entity in [args[0] for args, kwargs in check_can_verify_calls]
    assert (entity,
            user_input) in [args for args, kwargs in check_user_input_calls]
