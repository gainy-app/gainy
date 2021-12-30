import json
import os
import http.client
import urllib.parse
from aws_synthetics.selenium import synthetics_webdriver as syn_webdriver
from aws_synthetics.common import synthetics_logger as logger

HASURA_URL = os.getenv("HASURA_URL", "${hasura_url}")
HASURA_ADMIN_SECRET = os.getenv("HASURA_ADMIN_SECRET", "${hasura_admin_secret}")


def verify_request(method, url, post_data=None, headers={}):
    parsed_url = urllib.parse.urlparse(url)
    user_agent = str(syn_webdriver.get_canary_user_agent_string())
    if "User-Agent" in headers:
        headers["User-Agent"] = " ".join([user_agent, headers["User-Agent"]])
    else:
        headers["User-Agent"] = "{}".format(user_agent)

    logger.info("Making request with Method: '%s' URL: %s: Data: %s Headers: %s" % (
        method, url, json.dumps(post_data), json.dumps(headers)))

    if parsed_url.scheme == "https":
        conn = http.client.HTTPSConnection(parsed_url.hostname, parsed_url.port)
    else:
        conn = http.client.HTTPConnection(parsed_url.hostname, parsed_url.port)

    conn.request(method, url, str(post_data), headers)
    response = conn.getresponse()
    logger.info("Status Code: %s " % response.status)
    logger.info("Response Headers: %s" % json.dumps(response.headers.as_string()))

    if not response.status or response.status < 200 or response.status > 299:
        try:
            logger.error("Response: %s" % response.read().decode())
        finally:
            if response.reason:
                conn.close()
                raise Exception("Failed: %s" % response.reason)
            else:
                conn.close()
                raise Exception("Failed with status code: %s" % response.status)

    logger.info("Response: %s" % response.read().decode())
    logger.info("HTTP request successfully executed")
    conn.close()


def main():

    url1 = HASURA_URL
    method1 = 'POST'
    postData1 = json.dumps({
        "query":"{\n  collections(where: {enabled: {_eq: \"1\"}, _or: [{personalized: {_eq: \"1\"}, profile_id: {_eq: 8}}, {personalized: {_eq: \"0\"}}]}) {\n    id\n    name\n    enabled\n    personalized\n  }\n}\n",
        "variables":null
    })
    headers1 = {"x-hasura-admin-secret":HASURA_ADMIN_SECRET, "content-type":"application/json"}

    verify_request(method1, url1, postData1, headers1)

    logger.info("Canary successfully executed")


def handler(event, context):
    logger.info("Selenium Python API canary")
    main()