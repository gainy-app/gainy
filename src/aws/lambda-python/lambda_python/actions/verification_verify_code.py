from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.exceptions import BadRequestException, NotFoundException
from gainy.utils import get_logger
from verification.exceptions import CodeExpiredException, WrongCodeException, CodeAlreadyUsedException
from verification.models import VerificationCode

logger = get_logger(__name__)


class VerificationVerifyCode(HasuraAction):

    def __init__(self, action_name="verification_verify_code"):
        super().__init__(action_name)

    def apply(self, input_params, context_container: ContextContainer):
        verification_code_id = input_params["verification_code_id"]
        user_input = input_params["user_input"]

        verification_code: VerificationCode = context_container.get_repository(
        ).find_one(VerificationCode, {"id": verification_code_id})
        if not verification_code:
            raise NotFoundException()

        try:
            context_container.verification_service.verify_code(
                verification_code, user_input)
        except WrongCodeException:
            raise BadRequestException('Wrong code.')
        except CodeAlreadyUsedException:
            raise BadRequestException('Code has already been used.')
        except CodeExpiredException:
            raise BadRequestException('Code expired.')

        return {"ok": True}
