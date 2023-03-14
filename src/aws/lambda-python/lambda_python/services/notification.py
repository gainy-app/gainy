import os

from gainy.data_access.repository import Repository
from gainy.exceptions import NotFoundException, EmailNotSentException
from gainy.services.sendgrid import SendGridService
from gainy.utils import get_logger
from models import Profile

DW_MANAGER_EMAILS = os.getenv('DW_MANAGER_EMAILS', '').split(',')

logger = get_logger(__name__)


class NotificationService:

    def __init__(self, repository: Repository, sendgrid: SendGridService):
        self.repository = repository
        self.sendgrid = sendgrid

    def notify_dw_instrument_status_changed(self, symbol, status, new_status):
        subject = 'DriveWealth ticker %s changed status' % symbol
        text = 'DriveWealth ticker %s changed status from %s to %s' % (
            symbol, status, new_status)
        try:
            self.sendgrid.send_email(to=DW_MANAGER_EMAILS,
                                     subject=subject,
                                     content_plain=text)
        except EmailNotSentException as e:
            logger.exception(e, extra={"subject": subject, "text": text})

    def notify_dw_money_flow_status_changed(self, money_flow_type,
                                            money_flow_ref_id, old_status,
                                            new_status):
        subject = 'DriveWealth %s %s changed status' % (money_flow_type,
                                                        money_flow_ref_id)
        text = 'DriveWealth %s %s changed status from %s to %s' % (
            money_flow_type, money_flow_ref_id, old_status, new_status)
        try:
            self.sendgrid.send_email(to=DW_MANAGER_EMAILS,
                                     subject=subject,
                                     content_plain=text)
        except EmailNotSentException as e:
            logger.exception(e, extra={"subject": subject, "text": text})

    def notify_dw_account_status_changed(self, account_ref_id, old_status,
                                         new_status):
        subject = 'DriveWealth account %s changed status' % account_ref_id
        text = 'DriveWealth account %s changed status from %s to %s' % (
            account_ref_id, old_status, new_status)
        try:
            self.sendgrid.send_email(to=DW_MANAGER_EMAILS,
                                     subject=subject,
                                     content_plain=text)
        except EmailNotSentException as e:
            logger.exception(e, extra={"subject": subject, "text": text})

    def notify_low_balance(self, profile_id, balance):
        subject = 'Profile %d has low trading balance' % profile_id
        text = 'Profile %d has low trading balance: %f' % (profile_id, balance)
        try:
            self.sendgrid.send_email(to=DW_MANAGER_EMAILS,
                                     subject=subject,
                                     content_plain=text)
        except EmailNotSentException as e:
            logger.exception(e, extra={"subject": subject, "text": text})

    def on_kyc_status_approved(self, profile_id):
        logger.info('Sending notification on_kyc_status_approved',
                    extra={"profile_id": profile_id})
        return
        subject = ''
        self._notify_user(profile_id, subject)

    def on_kyc_status_rejected(self, profile_id):
        logger.info('Sending notification on_kyc_status_rejected',
                    extra={"profile_id": profile_id})
        return
        subject = ''
        self._notify_user(profile_id, subject)

    def on_kyc_status_info_required(self, profile_id):
        logger.info('Sending notification on_kyc_status_info_required',
                    extra={"profile_id": profile_id})
        return
        subject = ''
        self._notify_user(profile_id, subject)

    def on_kyc_status_doc_required(self, profile_id):
        logger.info('Sending notification on_kyc_status_doc_required',
                    extra={"profile_id": profile_id})
        return
        subject = ''
        self._notify_user(profile_id, subject)

    def on_funding_account_linked(self, profile_id):
        logger.info('Sending notification on_funding_account_linked',
                    extra={"profile_id": profile_id})
        return
        subject = ''
        self._notify_user(profile_id, subject)

    def on_deposit_initiated(self, profile_id):
        logger.info('Sending notification on_deposit_initiated',
                    extra={"profile_id": profile_id})
        return
        subject = ''
        self._notify_user(profile_id, subject)

    def on_deposit_success(self, profile_id):
        logger.info('Sending notification on_deposit_success',
                    extra={"profile_id": profile_id})
        return
        subject = ''
        self._notify_user(profile_id, subject)

    def on_withdraw_success(self, profile_id):
        logger.info('Sending notification on_withdraw_success',
                    extra={"profile_id": profile_id})
        return
        subject = ''
        self._notify_user(profile_id, subject)

    def _notify_user(self, profile_id, subject, plain_text=None):
        #TODO make async through sqs
        try:
            self.sendgrid.send_email(
                to=self._get_profile_notification_email(profile_id),
                subject=subject,
                content_plain=plain_text)
        except EmailNotSentException as e:
            logger.exception(e, extra={"subject": subject})

    def _get_profile_notification_email(self, profile_id):
        profile: Profile = self.repository.find_one(Profile,
                                                    {"id": profile_id})
        if not profile:
            raise NotFoundException()

        return profile.email
