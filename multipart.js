const program = require('commander')
const aws=require('aws-sdk')
const cp = require('child_process')
const os = require('os')
const fs=require('fs')
const url = require('url')
const path = require('path')
//const ProgressBar = require('ascii-progress');
const ProgressBar = require('progress')
var ncpus = os.cpus().length

const s3 = new aws.S3()


var start_time;

var fetchers = 10;
var temp = '/tmp/';

function makeCounter(limit, callback) {
	return function() {
		if (--limit === 0) {
			callback();
		}
	}
}


var data_chunk = [];

function download(params, chunks, fd) {
	var start_time = process.hrtime();
	var writing = false;

	var bar = new ProgressBar('  [:bar] :percent :etas', {
			complete: '=',
			incomplete: ' ',
			width: 60,
			total: chunks.upper-chunks.lower
		});
	bar.tick(0);

	var fetchers_count=Math.ceil(chunks.upper/chunks.size);

	var done =  makeCounter(fetchers_count, function() { 
		var end_time = process.hrtime(start_time); 
		bar.terminate();
		console.log(`Completed in ${end_time}s at ${chunks.upper/1024./1024/end_time[0]} Mibps`)
		//fs.close(fd);
	});


	var task = function(idx, args, task_done) {
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
					fs.write(fd,data.Body, 0, data.Body.length, this.lower, (err, written, buffer) => {
						if (err) {
							throw err;
						}
						writing = false;
						bar.tick(written);
					});
					task_done();
				}
			}.bind({lower:lower}));
		}.bind({p:params});
		f();
	}
	
	for (var k=0; k<fetchers_count; k++) {
		task(k, chunks, done)
	}
}


program.arguments('<s3object>')
	.arguments('<outputfile>')
	.option('-s, --size <size>', 'The size of the chunks to download')
	.option('-f, --fetchers <fetchers>', 'The number of asynchronous fetchers')
	.option('-t, --temp <temp>', 'Location of the temp files')
	.action((s3object, outputfile) => {
		var error_message;
		//console.log('Getting file size'+file);
		var s3uri = url.parse(s3object);
		if (s3uri.protocol !== 's3:') {
			error_message = `Not a valid S3 object ${s3object}`
		}

		if (error_message) {
			console.log(error_message);
			process.exit(1);
		}

		var params = {
			Bucket: s3uri.host,
			Key: s3uri.path.substr(1)
		}
		//console.log(params);
		s3.headObject(params, function(err, data) { 
			if (err) {
				console.log(`Error getting object from S3: ${err.code}`);
				process.exit(1); 
			}
			else {
				var size = parseInt(data.ContentLength);
				console.log("Total size = %d", size);
				var chunk_size = 2000000;
				if (program.size) {
					chunk_size = parseInt(program.size);
				}
				if (program.fetchers) {
					fetchers = parseInt(program.fetchers);
				}
				if (program.temp) {
					temp = program.temp;
				}

				console.log(`Chunk size = ${chunk_size}, Num procs: ${ncpus}`);
				var fd = fs.openSync(outputfile, 'w');
				fs.writeSync(fd,['\0'],0,1,size-1);			
				chunks = {
					'lower': 0, 
					'upper': size,
					'size': chunk_size,
					'fetchers': fetchers
				}
				download(params, chunks, fd);				
			}
		});
	})
	.parse(process.argv);


