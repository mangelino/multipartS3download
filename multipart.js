#! /usr/bin/env node 
"use strict";
exports.__esModule = true;
var program = require("commander");
var fs = require("fs");
var url = require("url");
var filesize = require("filesize");
var Promise = require("promise");
//import * as ProgressBar from 'ascii-progress';;
var ProgressBar = require("progress");
var aws_sdk_1 = require("aws-sdk");
var s3 = new aws_sdk_1.S3();
function makeCounter(limit, callback) {
    return function () {
        if (--limit === 0) {
            callback();
        }
    };
}
function download(params, chunks, fd) {
    var start_time = process.hrtime();
    // progress is not a top library, would have preferred ansi-progress but 
    // does not work in ssh remote terminals
    var fetchers_count = Math.ceil((chunks.upper - chunks.lower) / chunks.size);
    if (fetchers_count > 100) {
        fetchers_count = 100;
        chunks.size = Math.ceil((chunks.upper - chunks.lower) / fetchers_count);
        console.log("Max 100 fetchers, using Chunk size: " + filesize(chunks.size));
    }
    var bar = new ProgressBar('  [:bar] :percent :etas', {
        complete: '=',
        incomplete: ' ',
        width: 60,
        total: chunks.upper - chunks.lower
    });
    bar.tick(0);
    function writeAsync(fd, data, offset) {
        return new Promise(function (resolve, reject) {
            fs.write(fd, data, 0, data.length, offset, function (err, written, buffer) {
                if (err)
                    reject(err);
                resolve(written);
            });
        });
    }
    function getS3ObjectAsync(params, offset) {
        return new Promise(function (resolve, reject) {
            var req = s3.getObject(params, function (err, data) {
                if (err)
                    return reject(err);
                if (fd) {
                    writeAsync(fd, data.Body, offset)
                        .then(function (res) {
                        bar.tick(res);
                        resolve(res);
                    })["catch"](function (err) {
                        reject(err);
                    });
                }
                else {
                    bar.tick(data.Body.length);
                    resolve();
                }
            });
            // req.on('retry', function(response) {
            //     console.error(response.error.message + ":" + response.error.retryable);
            // }) 
        });
    }
    var tasks = [];
    for (var k = 0; k < fetchers_count; k++) {
        var lower = chunks.lower + k * chunks.size;
        var upper = Math.min(lower + chunks.size - 1, chunks.upper);
        params.Range = "bytes=" + lower + "-" + upper;
        tasks.push(getS3ObjectAsync(params, lower));
    }
    Promise.all(tasks)
        .then(function (res) {
        if (fd) {
            fs.closeSync(fd);
        }
        var end_time = process.hrtime(start_time);
        bar.terminate();
        var time_secs = (end_time[0] * 1000000000 + end_time[1]) / 1000000000;
        console.log("Download completed in " + time_secs.toFixed(2) + "s at " + filesize(chunks.upper / time_secs) + "ps");
    })["catch"](function (err) {
        console.error(err);
        process.exit(1);
    });
}
program
    .version('1.0.0')
    .usage('[options] <s3object> <outputfile>')
    .arguments('<s3object> <outputfile>')
    .option('-s, --size <size>', 'The size of the chunks to download', parseInt)
    .option('-t, --test', 'Test mode - does not write to disk')
    .option('-c, --credentials <credentials>', 'accessKey/secretAccessKey for the access to AWS S3')
    .action(function (s3object, outputfile) {
    if (typeof s3object === 'undefined') {
        console.error('An S3 URI must be provided');
        process.exit(1);
    }
    var error_message;
    var s3uri = url.parse(s3object);
    if (s3uri.protocol !== 's3:') {
        error_message = "Not a valid S3 object " + s3object;
    }
    if (program.credentials) {
        var c = program.credentials.split('/');
        if (c.length != 2) {
            error_message = 'Invalid credential. Must be specified as <accessKey>/<secretAccessKey>';
        }
        else {
            var aws_cred = new aws_sdk_1.Credentials(c[0], c[1]);
            aws_sdk_1.config.credentials = aws_cred;
            s3 = new aws_sdk_1.S3();
        }
    }
    if (error_message) {
        console.error(error_message);
        process.exit(1);
    }
    // Paramter object for S3 client
    var params = {
        Bucket: s3uri.host,
        Key: s3uri.path.substr(1)
    };
    s3.headObject(params, function (err, data) {
        if (err) {
            console.log("Error getting object from S3: " + err.code);
            process.exit(1);
        }
        else {
            var size = data.ContentLength;
            console.log("Total size = %s", filesize(size));
            var chunk_size = 2000000;
            if (program.size) {
                chunk_size = program.size;
            }
            var test = false;
            if (program.test)
                test = true;
            console.log("Chunk size = " + filesize(chunk_size));
            var fd = void 0;
            if (!test) {
                fd = fs.openSync(outputfile, 'w');
                fs.writeSync(fd, new Buffer(['\0']), 0, 1, size - 1);
            }
            var chunks = {
                'lower': 0,
                'upper': size,
                'size': chunk_size
            };
            download(params, chunks, fd);
        }
    });
});
program.parse(process.argv);
