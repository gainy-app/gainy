import datetime
import os

from gainy.data_access.operators import OperatorGt
from gainy.data_access.repository import Repository
from verification.client import VerificationClient
from verification.exceptions import CooldownException, CodeAlreadyUsedException, CodeExpiredException, \
    WrongCodeException
from verification.models import VerificationCodeChannel, VerificationCode

VERIFICATION_CODE_COOLDOWN = os.getenv("VERIFICATION_CODE_COOLDOWN")
VERIFICATION_CODE_TTL = os.getenv("VERIFICATION_CODE_TTL")


def _create_entity(profile_id: int, channel: VerificationCodeChannel,
                   address: str) -> VerificationCode:
    verification_code = VerificationCode()
    verification_code.profile_id = profile_id
    verification_code.channel = channel
    verification_code.address = address

    return verification_code


def _check_can_verify(entity: VerificationCode):
    if entity.failed_at is not None or entity.verified_at is not None:
        raise CodeAlreadyUsedException()

    if datetime.datetime.now(
            tz=datetime.timezone.utc) - entity.created_at > datetime.timedelta(
                seconds=int(VERIFICATION_CODE_TTL)):
        raise CodeExpiredException()


class VerificationService:

    def __init__(self,
                 repository: Repository,
                 clients: list[VerificationClient],
                 dry_run=False):
        self.repository = repository
        self.clients = clients
        self.dry_run = dry_run

    def send_code(self, profile_id: int, channel: VerificationCodeChannel,
                  address: str):
        entity = _create_entity(profile_id=profile_id,
                                channel=channel,
                                address=address)
        self._check_can_send(entity)
        self.repository.persist(entity)
        self._choose_client(entity).validate_address(entity)

        if not self.dry_run:
            self._choose_client(entity).send(entity)

        return entity

    def verify_code(self, entity: VerificationCode, user_input: str):
        _check_can_verify(entity)
        try:
            if not self.dry_run:
                self._choose_client(entity).check_user_input(
                    entity, user_input)

            entity.verified_at = datetime.datetime.now(
                tz=datetime.timezone.utc)
            self.repository.persist(entity)
        except WrongCodeException as e:
            entity.failed_at = datetime.datetime.now()
            self.repository.persist(entity)
            raise e

    def _check_can_send(self, entity: VerificationCode):
        created_at_threshold = datetime.datetime.now() - datetime.timedelta(
            seconds=int(VERIFICATION_CODE_COOLDOWN))
        old_entity = self.repository.find_one(
            VerificationCode, {
                "profile_id": entity.profile_id,
                "channel": entity.channel.name,
                "created_at": OperatorGt(created_at_threshold)
            })

        if old_entity:
            raise CooldownException()

    def _choose_client(self, entity: VerificationCode) -> VerificationClient:
        for client in self.clients:
            if client.supports(entity):
                return client
        raise Exception(f'No client for entity {entity.to_dict()}')
