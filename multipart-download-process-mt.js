var aws=require('aws-sdk')

var s3 = new aws.S3()


process.on('message', function(msg) {
	params = msg.params;
	chunks = msg.chunks;
	download(params, chunks)
})



function makeCounter(limit, callback) {
	return function() {
		if (--limit === 0) {
			callback();
		}
	}
}

function download(params, chunks) {
	var j=0, contentSize=chunks.lower;
	var start_time = process.hrtime();

	var done = makeCounter(Math.round((chunks.upper-chunks.lower)/chunks.size), function() { 
		var end_time = process.hrtime(start_time); 
		console.log("Download done for process "+process.pid+" in sec "+end_time);
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
					done();
				}
			}.bind({k:j}));
		}.bind({p:params});
		f();
		j++; 
		contentSize+=chunks.size;
	} while (contentSize<chunks.upper);
	console.log("PID %d: Started %d fetchers", process.pid,j)
}


