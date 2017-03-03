const aws=require('aws-sdk');
const fs=require('fs');


var s3 = new aws.S3();


process.on('message', function(msg) {
	params = msg.params;
	chunks = msg.chunks;
	download(params, chunks, 0)	
});



function makeCounter(limit, callback) {
	return function() {
		if (--limit === 0) {
			callback();
		}
	}
}

var data_chunk = [];

function download(params, chunks, fd) {
	var j=0, contentSize=chunks.lower;
	var start_time = process.hrtime();
	var chunks_n = Math.ceil((chunks.upper-chunks.lower)/chunks.size);
	var fname = chunks.folder+'/'+'chunk'+chunks.seq
	
	var process_done = function() { 
		var end_time = process.hrtime(start_time); 
		process.send({'type':'done'})
	};

	var fetchersSync = function(tot, batch, task, args, callback) {
		var fetchersCount = Math.min(batch, tot);
		var done = makeCounter(fetchersCount, () => {
			//console.log(`${chunks.seq}: Batch done ${Math.min(batch,tot)}`)
			for (i=0; i<fetchersCount; i++) {
					fs.appendFileSync(fname, data_chunk[i]);
			}
			//tar -console.log(`${chunks.seq}: Data appended`)
			tot -= batch;
			if (tot > 0) {
				// The args base must be incremented for each batch
				//console.log(`${chunks.seq}: Next batch`)
				args.lower += args.size*batch;
				fetchersSync(tot, batch, task, args, callback);
			} else {
				callback();
			}
		});

		//console.log(`${chunks.seq}: starting ${Math.min(tot, batch)} asynch tasks`);
		for (k=0; k<Math.min(batch, tot); k++) {
			task(k, args, done);
		}
	}
	
	var fetchers = function(tot, batch, task, args, callback) {
		var done = makeCounter(Math.min(batch, tot), () => {
			//console.log(`${chunks.seq}: Batch done ${Math.min(batch,tot)}`)
			
			var fappend=function(i, max, callback) {
				if (i<max) {
					console.log(`written ${args.seq} ${i}`)
					fs.appendFile(fname, data_chunk[i], () => {
						i++;
						fappend(i, max, callback);
					});
				} else {
					if (callback)
						callback();
				}
			}
			//fs.appendFile(fname, data_chunk[i], ;
			//console.log(`${chunks.seq}: Data appended`)
			tot -= batch;
			if (tot > 0) {
				fappend(0, Math.min(batch, tot), ()=>{console.log("Written")});
				// The args base must be incremented for each batch
				//console.log(`${chunks.seq}: Next batch`)
				args.lower += args.size*batch;
				fetchers(tot, batch, task, args, callback);
			} else {
				fappend(0, Math.min(batch, tot),
				callback());
			}
		});

		//console.log(`${chunks.seq}: starting ${Math.min(tot, batch)} asynch tasks`);
		for (k=0; k<Math.min(batch, tot); k++) {
			task(k, args, done);
		}
	}

	var task = function(idx, args, task_done) {
		//The idx indicates the chunk to download from the params base
		var lower = args.lower+idx*args.size;
		var upper = Math.min(lower+args.size-1, args.upper);

		params.Range = "bytes="+lower+"-"+upper;
		//console.log(`${chunks.seq}#${idx} `+params.Range)

		var f = function() {
			s3.getObject(this.p, 
			function(err, data) { 
				if (err) {
					console.log(err.code); 
					error=true;
					process.exit(1);
				} else { 
					//console.log(data);
					data_chunk[idx] = data.Body;
					process.send({'type':'tick','seq':args.seq ,'size':data.Body.length})
					task_done();
				}
			});
		}.bind({p:params});
		f();
	}
	//console.log(`${chunks.seq}: Total of  ${chunks_n} chunks\n ${chunks.lower}-${chunks.upper}`);
	fetchersSync(chunks_n, chunks.fetchers, task, chunks, process_done);

}


