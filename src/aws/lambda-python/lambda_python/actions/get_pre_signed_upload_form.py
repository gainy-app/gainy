import json
import logging
import os
import uuid
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from services import S3
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()

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

        with context_container.db_conn.cursor() as cursor:
            cursor.execute(
                """insert into app.uploaded_files(profile_id, s3_bucket, s3_key, content_type)
                values (%(profile_id)s, %(s3_bucket)s, %(s3_key)s, %(content_type)s)
                returning id""", {
                    "profile_id": profile_id,
                    "s3_bucket": s3_bucket,
                    "s3_key": s3_key,
                    "content_type": content_type,
                })
            file_id = cursor.fetchone()[0]

        return {
            'id': file_id,
            'method': 'put',
            'url': url,
        }
