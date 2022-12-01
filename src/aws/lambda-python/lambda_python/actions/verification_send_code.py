from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.exceptions import BadRequestException
from gainy.utils import get_logger
from verification.exceptions import CooldownException
from verification.models import VerificationCodeChannel

logger = get_logger(__name__)


class VerificationSendCode(HasuraAction):

    def __init__(self, action_name="verification_send_code"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]
        channel = input_params["channel"]
        address = input_params["address"]

        if channel not in list(VerificationCodeChannel):
            raise BadRequestException(
                f'Wrong channel {channel}. Options: {[e.value for e in VerificationCodeChannel]}'
            )

        try:
            verification_code = context_container.verification_service.send_code(
                profile_id=profile_id,
                channel=VerificationCodeChannel(channel),
                address=address)
        except CooldownException:
            raise BadRequestException(
                'You have requested a verification code too soon from the previous one.'
            )

        return {"verification_code_id": verification_code.id}
