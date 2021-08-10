const {"v4": uuidv4} = require('uuid');
var AWS = require('aws-sdk');
var s3 = new AWS.S3({
    signatureVersion: 'v4',
});

exports.handler = (event, context, callback) => {
    const url = s3.getSignedUrl('putObject', {
        Bucket: 'gainy-avatars',
        Key: uuidv4(),
        Expires: 3600,
    });

    callback(null, url);
};