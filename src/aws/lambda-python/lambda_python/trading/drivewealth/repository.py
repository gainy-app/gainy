from typing import Optional
from trading.drivewealth.models import DriveWealthDocument
from gainy.trading.drivewealth import DriveWealthRepository as GainyDriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthRepository(GainyDriveWealthRepository):

    def upsert_kyc_document(self, kyc_document_id: Optional[int],
                            data) -> DriveWealthDocument:
        id = data.get("id") or data.get("documentID")
        if not id:
            logger.error('Document without id', extra={"data": data})
            raise Exception('Document without id')

        entity = DriveWealthDocument()
        entity.ref_id = id
        entity.status = data["status"]["name"]
        entity.data = data

        if kyc_document_id:
            entity.kyc_document_id = kyc_document_id

        self.persist(entity)

        return entity
