#! /usr/bin/env node 

import * as program from 'commander';
import * as cp from 'child_process';
import * as os from 'os';
import * as fs from 'fs';
import * as url from 'url';
import * as path from 'path';
import * as filesize from 'filesize';
//import * as ProgressBar from 'ascii-progress';;
import * as ProgressBar from 'progress';
import { AWSError, S3, Config, Credentials, config } from "aws-sdk";

let s3 = new S3()

function makeCounter(limit: number, callback) {
	return function() {
		if (--limit === 0) {
			callback();
		}
	}
}

function download(params, chunks, fd: number): void {
	let start_time = process.hrtime();
	// progress is not a top library, would have preferred ansi-progress but 
	// does not work in ssh remote terminals
	let fetchers_count=Math.ceil((chunks.upper-chunks.lower)/chunks.size);
    if (fetchers_count > 100) {
        fetchers_count = 100;
        chunks.size = Math.ceil((chunks.upper-chunks.lower)/fetchers_count);
        console.log(`Max 100 fetchers, using Chunk size: ${filesize(chunks.size)}`)
    }

	let bar = new ProgressBar('  [:bar] :percent :etas', {
			complete: '=',
			incomplete: ' ',
			width: 60,
			total: chunks.upper-chunks.lower
		});
	bar.tick(0);

	let file_done = makeCounter(fetchers_count, function() { 
		fs.closeSync(fd);
		let end_time = process.hrtime(start_time); 
		bar.terminate();
		let time_secs = (end_time[0]*1000000000+end_time[1])/1000000000;
        console.log(`Download completed in ${time_secs.toFixed(2)}s at ${filesize(chunks.upper/time_secs)}ps`)
		
	});

	let task = function(idx: number, args) {
		//The idx indicates the chunk to download from the params base
		let lower = args.lower+idx*args.size;
		let upper = Math.min(lower+args.size-1, args.upper);

		params.Range = "bytes="+lower+"-"+upper;
		let p = params;

		s3.getObject(p, 
			function (err: AWSError, data) { 
				if (err) {
					console.log(err.code); 
					process.exit(1);
				} else { 
					if (fd) {
						fs.write(fd,data.Body as Buffer, 0, (data.Body as Buffer).length, lower, (err, written, buffer) => {
							if (err) {
								throw err;
							}
							bar.tick(written);
							file_done();
						});
					} else {
						bar.tick((data.Body as Buffer).length);
					}
				}
			});
	}
	// Starts all async fetcher tasks
	for (let k=0; k<fetchers_count; k++) {
		task(k, chunks)
	}
}

program
	.version('1.0.0')
	.usage('[options] <s3object> <outputfile>')
	.arguments('<s3object> <outputfile>')
	.option('-s, --size <size>', 'The size of the chunks to download', parseInt)
	.option('-t, --test', 'Test mode - does not write to disk')
	.option('-c, --credentials <credentials>', 'accessKey/secretAccessKey for the access to AWS S3')
	.action((s3object: string, outputfile: string) => {
		if (typeof s3object === 'undefined') {
			console.error('An S3 URI must be provided');
			process.exit(1);
		}
		let error_message: string;
		let s3uri = url.parse(s3object);
		if (s3uri.protocol !== 's3:') {
			error_message = `Not a valid S3 object ${s3object}`
		}
		if (program.credentials) {
			let c = program.credentials.split('/');
			if (c.length != 2) {
				error_message = 'Invalid credential. Must be specified as <accessKey>/<secretAccessKey>'
			} else {
				let aws_cred = new Credentials(c[0], c[1]);
				config.credentials = aws_cred;
				s3 = new S3()
			}
		}
		if (error_message) {
			console.error(error_message);
			process.exit(1);
		}
		
		// Paramter object for S3 client
		let params = {
			Bucket: s3uri.host,
			Key: s3uri.path.substr(1)
		}

		s3.headObject(params, function(err, data) { 
			if (err) {
				console.log(`Error getting object from S3: ${err.code}`);
				process.exit(1); 
			}
			else {
				let size = data.ContentLength;
				console.log("Total size = %s", filesize(size));
				let chunk_size = 2000000;
				if (program.size) {
					chunk_size = program.size;
				}
				let test = false;
				if (program.test)
					test = true;
				
				console.log(`Chunk size = ${filesize(chunk_size)}`);
				let fd;
				if (!test) {
					fd = fs.openSync(outputfile, 'w');
					fs.writeSync(fd,new Buffer(['\0']),0,1,size-1);
				}			
				let chunks = {
					'lower': 0, 
					'upper': size,
					'size': chunk_size
				}
				download(params, chunks, fd);				
			}
		});
	});

program.parse(process.argv);


