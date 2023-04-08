from gainy.context_container import ContextContainer

profile_id = 1


def run():
    with ContextContainer() as context_container:
        context_container.notification_service.on_kyc_status_approved(1)
        context_container.notification_service.on_kyc_status_rejected(1)
        context_container.notification_service.on_kyc_status_info_required(
            profile_id, 'some additional info')
        context_container.notification_service.on_kyc_status_doc_required(1)
        context_container.notification_service.on_funding_account_linked(
            profile_id, '****1111', 'Plaid Saving')
        context_container.notification_service.on_deposit_initiated(
            profile_id, 12345.67)
        context_container.notification_service.on_deposit_success(
            profile_id, 12345.67, '****1111')
        context_container.notification_service.on_deposit_failed(
            profile_id, 12345.67, '****1111')
        context_container.notification_service.on_withdraw_success(1, 12345.67)
        context_container.notification_service.on_kyc_form_abandoned(
            profile_id)
        context_container.notification_service.on_kyc_passed1(profile_id)
        context_container.notification_service.on_kyc_passed2(profile_id)
