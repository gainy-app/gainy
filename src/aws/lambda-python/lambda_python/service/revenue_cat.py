import os
from service.logging import get_logger
import requests

logger = get_logger(__name__)

REVENUECAT_API_KEY = os.getenv("REVENUECAT_API_KEY")

class RevenueCatService:
    def get_subscriber(self, profile_id):
        url = "https://api.revenuecat.com/v1/subscribers/{profile_id}"

        headers = {
            "Accept": "application/json",
            "X-Platform": "ios",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {REVENUECAT_API_KEY}"
        }

        response = requests.get(url, headers=headers)

        if r.status_code != 200:
            logger.error("RevenueCat get_subscriber response: %d %s", r.status_code, response.text)
            raise Exception("RevenueCat get_subscriber error")

        logger.debug("RevenueCat get_subscriber response: %s", response.text)

        return response.json()['subscriber']
