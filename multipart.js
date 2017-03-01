var aws=require('aws-sdk')

var s3 = new aws.S3()

var params = {
	Bucket: 'emr-workshop-maan',
	Key: 'input/mz.tgz'
}
var size;


function makeCounter(limit, callback) {
	return function() {
		if (--limit === 0) {
			callback();
		}
	}
}

function download() {
	var j=0, contentSize=0;
	var start = process.hrtime();

	var done = makeCounter(Math.round(size/10000000)-1, function() { 
		var end = process.hrtime(start); 
		console.log("Download done in: "+end);
	});

	console.log("Start download");
	do  { 
		var upper = contentSize+10000000-1; 
		if (upper>=size) 
			upper=size-1; 
		//console.log('%d:%d-%d',j,contentSize,upper); 
		params.Range = "bytes="+contentSize+"-"+upper; 
		var f = function() {
			s3.getObject(this.p, 
			function(err, data) { 
				if (err) {
					console.log(err.code); 
					error=true;
					process.exit(1);
				} else { 
					console.log("Completed chunk %d", this.k); 
					done();
				}
			}.bind({k:j}));
		}.bind({p:params});
		f();
		j++; 
		contentSize+=10000000;
	} while (contentSize<size);
	console.log("All parts download started")
}

console.log('Getting file size'+params);
s3.headObject(params, function(err, data) { 
	if (err) {
		console.log(err.code);
		process.exit(1); 
	}
	else {
		size = parseInt(data.ContentLength);
		console.log("Total size= %d", size);
		download();
	}
});