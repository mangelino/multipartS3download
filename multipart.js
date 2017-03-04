#! /usr/bin/env node 

const program = require('commander')
const aws=require('aws-sdk')
const cp = require('child_process')
const os = require('os')
const fs=require('fs')
const url = require('url')
const path = require('path')
const filesize = require('filesize')
//const ProgressBar = require('ascii-progress');
const ProgressBar = require('progress')
var ncpus = os.cpus().length

var s3 = new aws.S3()

function makeCounter(limit, callback) {
	return function() {
		if (--limit === 0) {
			callback();
		}
	}
}

function download(params, chunks, fd) {
	var start_time = process.hrtime();
	var writing = false;
	// progress is not a top library, would have preferred ansi-progress but 
	// does not work in ssh remote terminals
	var bar = new ProgressBar('  [:bar] :percent :etas', {
			complete: '=',
			incomplete: ' ',
			width: 60,
			total: chunks.upper-chunks.lower
		});
	bar.tick(0);

	var fetchers_count=Math.ceil(chunks.upper/chunks.size);
	var net_end_time;

	var file_done = makeCounter(fetchers_count, function() { 
		fs.closeSync(fd);
		var end_time = process.hrtime(start_time); 
		bar.terminate();
		console.log(`Download completed in ${end_time}s at ${chunks.upper/1024./1024/end_time[0]} Mibps`)
		
	});

	var task = function(idx, args) {
		//The idx indicates the chunk to download from the params base
		var lower = args.lower+idx*args.size;
		var upper = Math.min(lower+args.size-1, args.upper);

		params.Range = "bytes="+lower+"-"+upper;
		var f = function() {
			s3.getObject(this.p, 
			function(err, data) { 
				if (err) {
					console.log(err.code); 
					error=true;
					process.exit(1);
				} else { 
					writing = true;
					if (fd) {
						fs.write(fd,data.Body, 0, data.Body.length, this.lower, (err, written, buffer) => {
							if (err) {
								throw err;
							}
							writing = false;
							bar.tick(written);
							file_done();
						});
					} else {
						bar.tick(data.Body.length);
					}
				}
			}.bind({lower:lower}));
		}.bind({p:params});
		f();
	}
	// Starts all async fetcher tasks
	for (var k=0; k<fetchers_count; k++) {
		task(k, chunks)
	}
}

//TODO: Add access key and secret as optional parameters
program
	.version('1.0.0')
	.usage('[options] <s3object> <outputfile>')
	.arguments('<s3object> <outputfile>')
	.option('-s, --size <size>', 'The size of the chunks to download', parseInt)
	.option('-t, --test', 'Test mode - does not write to disk')
	.option('-c, --credentials <credentials>', 'accessKey/secretAccessKey for the access to AWS S3')
	.action((s3object, outputfile) => {
		if (typeof s3object === 'undefined') {
			console.error('An S3 URI must be provided');
			process.exit(1);
		}
		var error_message;
		//console.log('Getting file size'+file);
		var s3uri = url.parse(s3object);
		if (s3uri.protocol !== 's3:') {
			error_message = `Not a valid S3 object ${s3object}`
		}
		if (program.credentials) {
			var c = program.credentials.split('/');
			if (c.length != 2) {
				error_message = 'Invalid credential. Must be specified as <accessKey>/<secretAccessKey>'
			} else {
				var aws_cred = new aws.Credentials(c[0], c[1]);
				//var config = new aws.Config();
				//config.credentials = aws_cred;
				aws.config.credentials = aws_cred;
				s3 = new aws.S3()
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
		}

		s3.headObject(params, function(err, data) { 
			if (err) {
				console.log(`Error getting object from S3: ${err.code}`);
				process.exit(1); 
			}
			else {
				var size = parseInt(data.ContentLength);
				console.log("Total size = %s", filesize(size));
				var chunk_size = 2000000;
				if (program.size) {
					chunk_size = program.size;
				}
				var test = false;
				if (program.test)
					test = true;
				
				console.log(`Chunk size = ${filesize(chunk_size)}`);
				var fd;
				if (!test) {
					fd = fs.openSync(outputfile, 'w');
					fs.writeSync(fd,['\0'],0,1,size-1);
				}			
				chunks = {
					'lower': 0, 
					'upper': size,
					'size': chunk_size
				}
				download(params, chunks, fd);				
			}
		});
	});

program.parse(process.argv);


