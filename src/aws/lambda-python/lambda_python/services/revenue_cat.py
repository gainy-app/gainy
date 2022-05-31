import os
from services.logging import get_logger
import requests

logger = get_logger(__name__)

REVENUECAT_API_KEY = os.getenv("REVENUECAT_API_KEY")


class RevenueCatService:

    def get_subscriber(self, profile_id):
        url = f"https://api.revenuecat.com/v1/subscribers/{profile_id}"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {REVENUECAT_API_KEY}"
        }

        response = requests.get(url, headers=headers)

        if response.status_code not in [200, 201]:
            logger.error("RevenueCat get_subscriber response: %d %s",
                         response.status_code, response.text)
            raise Exception("RevenueCat get_subscriber error")

        logger.debug("RevenueCat get_subscriber response: %s", response.text)

        return response.json()['subscriber']
