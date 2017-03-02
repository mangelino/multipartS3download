var aws=require('aws-sdk')
var cp = require('child_process');
var os = require('os');

var ncpus = os.cpus().length;

var s3 = new aws.S3()

function makeCounter(limit, callback) {
	return function() {
		if (--limit === 0) {
			callback();
		}
	}
}

var start_time;

var done = makeCounter(ncpus, function() { 
	var end_time = process.hrtime(start_time); 
	console.log("Total download done in "+end_time);
	process.exit(0);
});

function start_child_processes(size, chunk_size, params) {
	var chunks_count = Math.round(size/chunk_size);
	var chunks_per_child = Math.round(chunks_count/ncpus);
	var chunks_started = 0;
	var current_size=0;
	start_time = process.hrtime();

	while (chunks_started < chunks_count-chunks_per_child) {
		var c = cp.fork(__dirname+"/multipart-download-process-mt.js");
		var chunks = { 'lower': current_size,
			'upper': current_size+chunk_size*chunks_per_child,
			'size': chunk_size}
		var msg = { 'params': params, 'chunks' : chunks}
		c.on('message', done)
		c.send(msg)
		chunks_started += chunks_per_child
		current_size += chunk_size*chunks_per_child
	}
	var c = cp.fork(__dirname+"/multipart-download-process-mt.js");
	var chunks = { 'lower': current_size,
			'upper': size,
			'size': chunk_size}
	var msg = { 'params': params, 'chunks' : chunks}
	c.send(msg)
	console.log('All child process started');
}

var params = {
			Bucket: 'emr-workshop-maan',
			Key: 'input/mz.tgz'
		}
console.log('Getting file size'+params);
s3.headObject(params, function(err, data) { 
	if (err) {
		console.log(err.code);
		process.exit(1); 
	}
	else {
		var size = parseInt(data.ContentLength);
		console.log("Total size= %d", size);
		var chunk_size = 10000000;
		start_child_processes(size, chunk_size, params);
		

	}
});