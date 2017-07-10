'use strict';

const
    Promise = require('bluebird'),
    bunyan = require('bunyan'),
	_ = require('lodash'),
    crypto = require("crypto"),
    retry = require("retry-bluebird"),
    AWS = require('aws-sdk');

const logger = bunyan.createLogger({
    name: 's3-read',
    level: 'info',
    serializers: {
        err: bunyan.stdSerializers.err
    },
});

const config = {
    bucket: "bcrouchman-s3-perf-test",
    numFolders: 100,
    region: "us-east-1",
    folderSize: 3,
    hashChars: 6
};

const Bucket = config.bucket;

let s3 = Promise.promisifyAll(new AWS.S3(config.region));

function listObjects(prefix, continuationToken) {
    let params = {
        Bucket,
        Prefix: prefix
    };
    if(continuationToken !== undefined) {
        params.ContinuationToken = continuationToken;
    }
    return retry({backoff: 200, max: 10}, (() => {
        return s3.listObjectsV2Async(params)
    })).then((data) => {
        console.log(`first: ${data.Contents[0].Key}`);
        console.log(`last: ${data.Contents[data.Contents.length - 1].Key}`);
        return data;
    })
}


function readObjects(prefix, startKey, contents) {
    logger.debug('Starting to process');
    let numSuccessful = 0;
    let numFailed = 0;

    logger.info({prefix, startKey}, 'Starting to read');
    return Promise.map(contents, (content) => {
        let key = content.Key;
        //let folder = crypto.createHash('md5').update(id.toString()).digest('hex').substring(0, config.hashChars);
        return retry({backoff: 100, max: 3}, (() => {
            return s3.getObjectAsync({Bucket, Key: `${key}`});
        })).then(() => {
            numSuccessful++;
            logger.debug({key}, 'read successful');
        }).catch((err) => {
            numFailed++;
            logger.error({key}, '===FAILED=== read');
        });
    }, {concurrency: 100}).then(() => {
        logger.info({prefix, startKey, numSuccessful, numFailed}, 'Finish reading ....');
    });
}


function replayWithPrefixes(prefixes) {
    return Promise.map(prefixes, prefix => {
        let continuationToken;
        let startKey;
        return listObjects(prefix, continuationToken)
            .then((data) => {
                logger.info('data.IsTruncated', data.IsTruncated);
                continuationToken = data.IsTruncated ? data.NextContinuationToken : null;
                if (data.KeyCount > 0) {
                    return readObjects(prefix, data.StartAfter, data.Contents);
                } else {
                    return true;
                }
            })
            .then(() => {
                if (continuationToken) {
                    return listObjects(prefix, continuationToken);
                }
            });
    }, {concurrency: 10});
}

const prefixes = [];
for (let i = 0; i < 10; i++) {
    prefixes.push(('000' + i.toString(16)).substr(-3));
}
logger.info('prefixes', prefixes);
replayWithPrefixes(prefixes)
    .then(() => {
        logger.info('completed');
    });
