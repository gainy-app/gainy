import os
import uuid
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger
from models import UploadedFile
from services.aws_s3 import S3

logger = get_logger(__name__)

BUCKETS = {"kyc": os.getenv('S3_BUCKET_UPLOADS_KYC')}


class GetPreSignedUploadForm(HasuraAction):

    def __init__(self):
        super().__init__("get_pre_signed_upload_form", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]
        upload_type = input_params["upload_type"]
        content_type = input_params["content_type"]

        if upload_type not in BUCKETS:
            raise Exception('Wrong type')

        s3_bucket = BUCKETS[upload_type]

        service = S3()

        s3_key = str(uuid.uuid4())
        url = service.get_pre_signed_upload_form(s3_bucket, s3_key)

        uploaded_file = UploadedFile()
        uploaded_file.profile_id = profile_id
        uploaded_file.s3_bucket = s3_bucket
        uploaded_file.s3_key = s3_key
        uploaded_file.content_type = content_type
        context_container.get_repository().persist(uploaded_file)

        return {
            'id': uploaded_file.id,
            'method': 'put',
            'url': url,
        }
