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

    def symbol_is_in_collection(self, symbol: str) -> bool:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                '''select exists(select ttf_name 
                                 from raw_data.ticker_collections_weights
                                 where symbol = %(symbol)s
                                   and _sdc_extracted_at > (select max(_sdc_extracted_at) from raw_data.ticker_collections_weights) - interval '1 hour') 
                ''', {"symbol": symbol})
            return cursor.fetchone()[0]
