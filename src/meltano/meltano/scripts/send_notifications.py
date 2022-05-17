import json
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import uuid
from gainy.utils import db_connect, get_logger

psycopg2.extras.register_uuid()

API_KEY = os.environ['ONESIGNAL_API_KEY']
APP_ID = os.environ['ONESIGNAL_APP_ID']
SEGMENTS = json.loads(os.environ['ONESIGNAL_SEGMENTS'])

logger = get_logger(__name__)


def send_push_notification(notification):
    header = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Basic {API_KEY}"
    }

    payload = {
        "app_id": APP_ID,
        "contents": notification['text'],
        "data": notification['data']
    }

    if notification['email']:
        payload['filters'] = [{
            "field": "email",
            "relation": "=",
            "value": notification['email']
        }]
    else:
        payload['included_segments'] = SEGMENTS

    return requests.post("https://onesignal.com/api/v1/notifications",
                         headers=header,
                         data=json.dumps(payload))


def send_all(sender_id):
    with db_connect() as db_conn:
        with db_conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """insert into app.notifications(profile_id, uniq_id, send_at, text, data, sender_id)
                   select profile_id, uniq_id, send_at, text, data, %(sender_id)s
                   from push_notifications
                   where send_at >= now()
                   on conflict do nothing""", {"sender_id": sender_id})
            cursor.execute(
                """select profiles.email, uuid, send_at, text, data
                   from app.notifications
                   left join app.profiles on profiles.id = notifications.profile_id
                   where sender_id = %(sender_id)s
                     and response is null""", {"sender_id": sender_id})

            for row in cursor:
                logger.debug("Sending push notification %s", dict(row))
                response = send_push_notification(row)
                response_serialized = {
                    "status_code": response.status_code,
                    "data": response.json()
                }
                if response.status_code == 200:
                    logger.debug("Onesignal response: %s", response_serialized)
                else:
                    logger.error("Failed to send notification: %s",
                                 response_serialized)

                with db_conn.cursor() as update_cursor:
                    update_cursor.execute(
                        """update app.notifications set response = %(response)s
                           where uuid = %(notification_uuid)s""", {
                            "response": json.dumps(response_serialized),
                            "notification_uuid": row["uuid"]
                        })


send_all(uuid.uuid4())
