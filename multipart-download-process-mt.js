var aws=require('aws-sdk')
var fs=require('fs')

var s3 = new aws.S3()


process.on('message', function(msg) {
	params = msg.params;
	chunks = msg.chunks;
	folder = chunks.folder;
	var fname = folder+'/'+'chunk'+chunks.seq
	fs.writeFileSync(fname, new Buffer(chunks.upper-chunks.lower))
	var fd = fs.openSync(fname, 'w');
	download(params, chunks,fd);
	
	
	
	
})



function makeCounter(limit, callback) {
	return function() {
		if (--limit === 0) {
			callback();
		}
	}
}

var data_chunk = []

function download(params, chunks, fd) {
	var j=0, contentSize=chunks.lower;
	var start_time = process.hrtime();

	var done = makeCounter(Math.round((chunks.upper-chunks.lower)/chunks.size), function() { 
		var end_time = process.hrtime(start_time); 
		console.log("Partial download done for process "+process.pid+" in sec "+end_time);
		console.log(data_chunk[0])
		process.send({'result':'Done'})
	});

	//console.log("Start download");
	do  { 
		var upper = contentSize+chunks.size-1; 
		if (upper>chunks.upper) 
			upper=chunks.upper; 
		//console.log('%s: %d-%d',process.pid,contentSize,upper); 
		params.Range = "bytes="+contentSize+"-"+upper; 
		var f = function() {
			s3.getObject(this.p, 
			function(err, data) { 
				if (err) {
					console.log(err.code); 
					error=true;
					process.exit(1);
				} else { 
					//console.log("Completed chunk %d", this.k); 
					data_chunk[this.k] = data.Body;
					fs.writeSync(fd, data.Body, 0, data.Body.length, this.lowerBound-chunks.lower)
					console.log(`Fetcher ${this.k}`)
					done();
				}
			}.bind({k:j}));
		}.bind({p:params, lowerBound:contentSize});
		f();
		j++; 
		contentSize+=chunks.size;
	} while (contentSize<chunks.upper);
	console.log("PID %d: Started %d fetchers", process.pid,j)
}


