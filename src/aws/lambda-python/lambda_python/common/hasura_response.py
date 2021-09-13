import json

def base_response(status_code, body=None):
    return {
        "isBase64Encoded": False,
        "statusCode": status_code,
        "headers": {},
        "body": json.dumps(body, default=str),
    }

def error_response(status_code, message):
    return base_response(status_code, {'error': message})

def unauthorized(message='Unauthorized'):
    return error_response(401, message)

def bad_request(message='Bad Request'):
    return error_response(400, message)

def success(body):
    return base_response(200, body)
