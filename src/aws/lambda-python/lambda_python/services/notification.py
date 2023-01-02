import os

from gainy.utils import get_logger
from services.sendgrid import SendGridService

DW_MANAGER_EMAILS = os.getenv('DW_MANAGER_EMAILS', '').split(',')

logger = get_logger(__name__)


class NotificationService:

    def __init__(self, sendgrid: SendGridService):
        self.sendgrid = sendgrid

    def notify_dw_instrument_status_changed(self, symbol, status, new_status):
        subject = 'DriveWealth ticker %s changed status' % symbol
        text = 'DriveWealth ticker %s changed status from %s to %s' % (
            symbol, status, new_status)
        self.sendgrid.send_email(to=DW_MANAGER_EMAILS,
                                 subject=subject,
                                 content_plain=text)

    def notify_low_balance(self, profile_id, balance):
        subject = 'Profile %d has low trading balance' % profile_id
        text = 'Profile %d has low trading balance: %f' % (profile_id, balance)
        self.sendgrid.send_email(to=DW_MANAGER_EMAILS,
                                 subject=subject,
                                 content_plain=text)
