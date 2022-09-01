import io
from services import S3
from managed_portfolio.models import KycDocument
from managed_portfolio.drivewealth import DriveWealthProvider
from managed_portfolio.repository import ManagedPortfolioRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class ManagedPortfolioService:

    def send_kyc_form(self, context_container, kyc_form: dict):
        return self.get_provider_service().send_kyc_form(
            context_container, kyc_form)

    def get_kyc_status(self, context_container, profile_id: int):
        return self.get_provider_service().get_kyc_status(
            context_container, profile_id)

    def send_kyc_document(self, context_container, profile_id: int,
                          document: KycDocument):
        with context_container.db_conn.cursor() as cursor:
            cursor.execute(
                """select s3_bucket, s3_key, content_type from app.uploaded_files
                where profile_id = %(profile_id)s and id = %(id)s""", {
                    "profile_id": profile_id,
                    "id": document.uploaded_file_id,
                })
            row = cursor.fetchone()

        if row is None:
            raise Exception('File not Found')
        (s3_bucket, s3_key, content_type) = row
        document.content_type = content_type

        repository = ManagedPortfolioRepository(context_container)
        repository.upsert_kyc_document(profile_id, document)

        file_stream = io.BytesIO()
        S3().download_file(s3_bucket, s3_key, file_stream)

        return self.get_provider_service().send_kyc_document(
            context_container, profile_id, document, file_stream)

    def get_provider_service(self):
        return DriveWealthProvider()
