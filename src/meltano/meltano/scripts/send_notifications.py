import json
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import uuid
from gainy.utils import db_connect, get_logger, env

psycopg2.extras.register_uuid()

API_KEY = os.environ['ONESIGNAL_API_KEY']
APP_ID = os.environ['ONESIGNAL_APP_ID']
ENV = env()
SEGMENTS = {
    'production':
    json.loads(os.environ.get('ONESIGNAL_SEGMENTS_PRODUCTION', '[]')),
    'test': json.loads(os.environ.get('ONESIGNAL_SEGMENTS_TEST', '[]')),
    'local': [],
}
EMAILS_LOCAL = json.loads(os.environ.get('ONESIGNAL_EMAILS_LOCAL', '[]'))

logger = get_logger(__name__)


def pick_target(notification):
    if notification['email']:
        return ([notification['email']], None)

    if ENV == 'local':
        return (EMAILS_LOCAL, None)

    env = ENV
    if ENV == 'production' and notification['is_test']:
        env = 'test'

    segments = SEGMENTS.get(env, [])

    return (None, segments)


def send_push_notification(notification):
    logger.debug("Sending push notification %s", dict(notification))

    header = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Basic {API_KEY}"
    }

    payload = {
        "app_id": APP_ID,
        "contents": notification['text'],
        "data": notification['data'],
        "isIos": True,
    }

    if notification['template_id']:
        payload['template_id'] = notification['template_id']

    (emails, segments) = pick_target(notification)

    logger.debug("Picked segments %s, emails %s for notification %s",
                 json.dumps(segments), json.dumps(emails),
                 notification['uuid'])

    if emails:
        payload['filters'] = [{
            "field": "email",
            "relation": "=",
            "value": email
        } for email in emails]
    elif segments:
        payload['included_segments'] = segments
    else:
        logger.error("Empty target for notification %s", notification['uuid'])

    return requests.post("https://onesignal.com/api/v1/notifications",
                         headers=header,
                         data=json.dumps(payload))


def send_all(sender_id):
    with db_connect() as db_conn:
        with db_conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """insert into app.notifications(profile_id, uniq_id, send_at, text, data, sender_id, is_test, template_id)
                   select profile_id, uniq_id, send_at, text, data, %(sender_id)s, is_test, template_id
                   from push_notifications
                   where send_at <= now()
                   on conflict do nothing""", {"sender_id": sender_id})
            cursor.execute(
                "update app.notifications set sender_id = %(sender_id)s where sender_id is null",
                {"sender_id": sender_id})
            cursor.execute(
                """select profiles.email, uuid, send_at, text, data, is_test, template_id
                   from app.notifications
                   left join app.profiles on profiles.id = notifications.profile_id
                   where sender_id = %(sender_id)s
                     and response is null""", {"sender_id": sender_id})

            for row in cursor:
                response = None
                try:
                    response = send_push_notification(row)

                    if response is None:
                        continue

                    response_serialized = {
                        "status_code": response.status_code,
                        "data": response.json()
                    }
                    if response.status_code == 200:
                        logger.debug("Onesignal response: %s",
                                     response_serialized)
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
                except Exception as e:
                    if response is not None:
                        logger.error(
                            "OneSignal send_push_notification error: %d %s",
                            response.status_code, response.text)

                    logger.exception(e)


send_all(uuid.uuid4())
