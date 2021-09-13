const response = (statusCode) => ({
  isBase64Encoded: false,
  statusCode,
  headers: {},
});
const errorResponse = (statusCode, error) => ({
  ...response(400),
  body: JSON.stringify({ error }),
});

exports.badRequestResponse = (message) => errorResponse(400, message);
exports.successfulResponse = (data) => ({
  ...response(200),
  body: JSON.stringify(data),
});
