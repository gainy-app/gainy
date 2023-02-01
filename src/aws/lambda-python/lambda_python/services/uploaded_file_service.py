from io import IOBase

from gainy.utils import get_logger
from models import UploadedFile
from services.aws_s3 import S3

logger = get_logger(__name__)


class UploadedFileService:

    def remove_file(self, uploaded_file: UploadedFile):
        S3().remove_file(uploaded_file.s3_bucket, uploaded_file.s3_key)

    def download_to_stream(self, uploaded_file: UploadedFile,
                           file_stream: IOBase):
        S3().download_file(uploaded_file.s3_bucket, uploaded_file.s3_key,
                           file_stream)
