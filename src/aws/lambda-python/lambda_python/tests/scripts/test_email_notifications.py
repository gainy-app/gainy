from gainy.context_container import ContextContainer


def run():
    with ContextContainer() as context_container:
        context_container.notification_service.on_kyc_status_approved(1)
        context_container.notification_service.on_kyc_status_rejected(1)
        context_container.notification_service.on_kyc_status_info_required(
            1, 'some additional info')
        context_container.notification_service.on_kyc_status_doc_required(1)
        context_container.notification_service.on_funding_account_linked(
            1, '****1111', 'Plaid Saving')
        context_container.notification_service.on_deposit_initiated(
            1, 12345.67)
        context_container.notification_service.on_deposit_success(
            1, 12345.67, '****1111')
        context_container.notification_service.on_deposit_failed(
            1, 12345.67, '****1111')
        context_container.notification_service.on_withdraw_success(1, 12345.67)
