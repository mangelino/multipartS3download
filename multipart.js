#! /usr/bin/env node 
"use strict";
exports.__esModule = true;
var program = require("commander");
var fs = require("fs");
var url = require("url");
var filesize = require("filesize");
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
    var file_done = makeCounter(fetchers_count, function () {
        fs.closeSync(fd);
        var end_time = process.hrtime(start_time);
        bar.terminate();
        var time_secs = (end_time[0] * 1000000000 + end_time[1]) / 1000000000;
        console.log("Download completed in " + time_secs.toFixed(2) + "s at " + filesize(chunks.upper / time_secs) + "ps");
    });
    var task = function (idx, args) {
        //The idx indicates the chunk to download from the params base
        var lower = args.lower + idx * args.size;
        var upper = Math.min(lower + args.size - 1, args.upper);
        params.Range = "bytes=" + lower + "-" + upper;
        var p = params;
        s3.getObject(p, function (err, data) {
            if (err) {
                console.log(err.code);
                process.exit(1);
            }
            else {
                if (fd) {
                    fs.write(fd, data.Body, 0, data.Body.length, lower, function (err, written, buffer) {
                        if (err) {
                            throw err;
                        }
                        bar.tick(written);
                        file_done();
                    });
                }
                else {
                    bar.tick(data.Body.length);
                }
            }
        });
    };
    // Starts all async fetcher tasks
    for (var k = 0; k < fetchers_count; k++) {
        task(k, chunks);
    }
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
