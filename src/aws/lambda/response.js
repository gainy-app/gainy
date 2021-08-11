const response = (statusCode) => {
    return {
        isBase64Encoded : false,
        statusCode,
        headers: { },
    };
};
const errorResponse = (statusCode, error) => {
    return {
        ...response(400),
        body: JSON.stringify({error})
    };
}

exports.badRequestResponse = (message) => errorResponse(400, message);
exports.successfulResponse = (data) => {
    return {
        ...response(200),
        "body": JSON.stringify(data)
    };
}