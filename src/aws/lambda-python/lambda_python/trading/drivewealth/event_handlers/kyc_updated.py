from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from trading.drivewealth.models import DriveWealthKycStatus
from trading.models import ProfileKycStatus

logger = get_logger(__name__)

DETAILS_MAPPING = [
    {
        "name":
        "AGE_VALIDATION",
        "code":
        "K001",
        "description":
        "The age calculated from the documents date of birth point is greater than or equal to the minimum accepted age set at the account level"
    },
    {
        "name":
        "POOR_PHOTO_QUALITY",
        "code":
        "K002",
        "description":
        "Poor photo quality. ID may be too dark, damaged, blurry, cut off, or have a glare"
    },
    {
        "name":
        "POOR_DOC_QUALITY",
        "code":
        "K003",
        "description":
        "Abnormal document quality. ID may have obscured data points, obscured security features, a corner removed, punctures, or watermarks obscured by digital text overlay"
    },
    {
        "name": "SUSPECTED_DOCUMENT_FRAUD",
        "code": "K004",
        "description": "Tampering and forgery found on the document"
    },
    {
        "name":
        "INCORRECT_SIDE",
        "code":
        "K005",
        "description":
        "The incorrect side of the document had been uploaded. Choose the correct side of the document and re-upload"
    },
    {
        "name":
        "NO_DOC_IN_IMAGE",
        "code":
        "K006",
        "description":
        "No document was found in the image, or there is a blank image"
    },
    {
        "name":
        "TWO_DOCS_UPLOADED",
        "code":
        "K007",
        "description":
        "Two different documents were submitted as the same document type"
    },
    {
        "name": "EXPIRED_DOCUMENT",
        "code": "K008",
        "description": "Document is expired or invalid format of expiry date"
    },
    {
        "name": "MISSING_BACK",
        "code": "K009",
        "description": "The back of the document is missing"
    },
    {
        "name": "UNSUPPORTED_DOCUMENT",
        "code": "K010",
        "description": "Document is not supported"
    },
    {
        "name":
        "DOB_NOT_MATCH_ON_DOC",
        "code":
        "K011",
        "description":
        "The DOB listed on the customer’s ID is not the same DOB listed on the customer’s application"
    },
    {
        "name":
        "NAME_NOT_MATCH_ON_DOC",
        "code":
        "K012",
        "description":
        "The Name on the the customer’s ID is not the same Name listed on the customer’s application"
    },
    {
        "name":
        "INVALID_DOCUMENT",
        "code":
        "K050",
        "description":
        "Unable to process your document. File is corrupted and can't be opened."
    },
    {
        "name": "ADDRESS_NOT_MATCH",
        "code": "K101",
        "description":
        "No match found for address or invalid format in address"
    },
    {
        "name": "SSN_NOT_MATCH",
        "code": "K102",
        "description": "No match found for Social Security Number"
    },
    {
        "name": "DOB_NOT_MATCH",
        "code": "K103",
        "description": "No match found for Date of Birth"
    },
    {
        "name":
        "NAME_NOT_MATCH",
        "code":
        "K104",
        "description":
        "No match found for firstName / lastName or invalid characters found"
    },
    {
        "name": "SANCTION_WATCHLIST",
        "code": "K106",
        "description": "User is under sanction watchlist"
    },
    {
        "name": "SANCTION_OFAC",
        "code": "K107",
        "description": "User is found in OFAC SDN list"
    },
    {
        "name":
        "INVALID_PHONE_NUMBER",
        "code":
        "K108",
        "description":
        "The phone number listed on the customer’s application is not a valid number of digits for a phone number"
    },
    {
        "name":
        "INVALID_EMAIL_ADDRESS",
        "code":
        "K109",
        "description":
        "The emailID listed on the customer’s application is not valid or unable to verify in Watchlist."
    },
    {
        "name":
        "INVALID_NAME_TOO_LONG",
        "code":
        "K110",
        "description":
        "The first name or last name listed on the customer's application is not valid. First name or last name should not be greater than 36 characters."
    },
    {
        "name": "UNSUPPORTED_COUNTRY",
        "code": "K111",
        "description": "The KYC is not supported in the country."
    },
    {
        "name": "AGED_ACCOUNT",
        "code": "K801",
        "description": "KYC is not verified by user within 30 days"
    },
    {
        "name":
        "ACCOUNT_INTEGRITY",
        "code":
        "K802",
        "description":
        "Account information provided may not be legitimate and/or is being used by multiple account holders"
    },
    {
        "name": "UNKNOWN",
        "code": "U999",
        "description": "Unrecognized error"
    },
]


def _get_profile_kyc_status(data) -> ProfileKycStatus:
    status = DriveWealthKycStatus.map_dw_kyc_status(data['status'])
    message = data['statusMessage']
    details = data.get('details', [])
    error_messages = []
    for name in details:
        mapping = list(filter(lambda x: x["name"] == name, DETAILS_MAPPING))
        if not mapping:
            continue
        error_messages.append(mapping[0]["description"])

    entity = ProfileKycStatus()
    entity.status = status
    entity.message = message
    entity.error_messages = error_messages

    return entity


class KycUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == "kyc.updated"

    def handle(self, event_payload: dict):
        user_id = event_payload["userID"]
        profile_id = self.provider.get_profile_id_by_user_id(user_id)
        entity = _get_profile_kyc_status(event_payload['current'])
        entity.profile_id = profile_id
        self.repo.persist(entity)
