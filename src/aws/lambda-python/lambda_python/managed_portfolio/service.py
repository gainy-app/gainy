from managed_portfolio.drivewealth import DriveWealthProvider


class ManagedPortfolioService:

    def send_kyc_form(self, context_container, kyc_form: dict):
        return self.get_provider_service().send_kyc_form(
            context_container, kyc_form)

    def get_kyc_status(self, context_container, profile_id: int):
        return self.get_provider_service().get_kyc_status(
            context_container, profile_id)

    def get_provider_service(self):
        return DriveWealthProvider()
