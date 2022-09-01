import boto3
from botocore.config import Config
from gainy.utils import get_logger

logger = get_logger(__name__)

PRE_SIGN_FORM_TTL = 600  # 10 minutes


class S3:

    def __init__(self):
        self.s3_client = boto3.client("s3",
                                      config=Config(signature_version='s3v4'))

    def get_pre_signed_upload_form(self, s3_bucket, s3_key):
        return self.s3_client.generate_presigned_url(
            ClientMethod='put_object',
            Params={
                'Bucket': s3_bucket,
                'Key': s3_key
            },
            ExpiresIn=PRE_SIGN_FORM_TTL,
        )

    def download_file(self, s3_bucket, s3_key, destination):
        logger.info('Downloading file %s %s', s3_bucket, s3_key)

        return self.s3_client.download_fileobj(s3_bucket, s3_key, destination)
