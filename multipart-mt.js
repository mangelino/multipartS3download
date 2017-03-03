const program = require('commander')
const aws=require('aws-sdk')
const cp = require('child_process')
const os = require('os')
const fs=require('fs')
const url = require('url')
//const ProgressBar = require('ascii-progress');
const multiProgress = require('multi-progress')
var ncpus = os.cpus().length

const s3 = new aws.S3()

var multi = new multiProgress();
var start_time;

var fetchers = 10;

function makeCounter(limit, callback) {
	return function() {
		if (--limit === 0) {
			callback();
		}
	}
}


function start_child_processes(size, chunk_size, params, temp_folder, outputfile) {
	var chunks_count = Math.round(size/chunk_size);
	var chunks_per_child = Math.round(chunks_count/ncpus);
	var chunks_started = 0;
	var current_size=0;
	start_time = process.hrtime();
	var k=0;
	var bars = Array()
	


	var done = makeCounter(ncpus, function() { 
		// Joins the partial files
		console.log('Merging temp files');
		fs.writeFileSync(outputfile, new Buffer(0));
		for (var i=0; i<ncpus; i++) {
			fs.appendFileSync(outputfile, fs.readFileSync(`${temp_folder}/chunk${i}`))
		}
		// Remove the temp_folder
		
		fs.readdirSync(temp_folder).forEach((file, index) => {
			var curPath = temp_folder+'/'+file;
			fs.unlinkSync(curPath);
		})
		fs.rmdirSync(temp_folder);
		
		var end_time = process.hrtime(start_time); 
		console.log("Total download done in "+end_time);
		process.exit(0);
	});


	var spawnChild = (lower, upper, seq) => {
		var child = cp.fork(__dirname+"/multipart-download-process-mt.js");
		var chunks = { 'lower': lower,
			'upper': upper,
			'size': chunk_size,
			'seq': seq,
			'folder': temp_folder,
			'fetchers': fetchers 
			}

		// bars.push(new ProgressBar({schema: ' [:bar] :percent :etas',
		// 	filled: '=',
		// 	width: 40,
		// 	total: upper-lower
		// }))	;

		bars.push(multiProgress.newBar('  [:bar] :percent :etas', {
			complete: '=',
			incomplete: ' ',
			width: 40,
			total: upper-lower
		}))
		bars[seq].tick(0);
		var completed = function(msg) {
			if (msg.type === 'tick') {
				bars[msg.seq].tick(msg.size);
			} else if (msg.type === 'done') {	
				done();
			}
		};

		var msg = { 'params': params, 'chunks' : chunks}
		child.on('message', completed);
		child.send(msg)
	}

	while (chunks_started < chunks_count-chunks_per_child) {

		spawnChild(current_size, current_size+chunk_size*chunks_per_child, k);

		chunks_started += chunks_per_child
		current_size += chunk_size*chunks_per_child
		k++;
	}
	// Last chunk must adapt the upper range
	spawnChild(current_size, size, k);

	//console.log('All child process started');
}

program.arguments('<s3object>')
	.arguments('<outputfile>')
	.option('-s, --size <size>', 'The size of the chunks to download')
	.option('-p, --processes <processes>', 'The number of processes. Will be limited to the number of cpus')
	.option('-f, --fetchers <fetchers>', 'The number of asynchronous fetchers')
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
				if (program.processes) {
					ncpus = parseInt(program.processes);
				}
				if (program.fetchers) {
					fetchers = parseInt(program.fetchers);
				}
				console.log(`Chunk size = ${chunk_size}, Num procs: ${ncpus}`)
				fs.mkdtemp('/tmp/mtp-mt-', (err, folder) => {
					if (err) {
						console.log(err);
						process.exit(1);
					}
					//console.log('Writing to '+folder)
					start_child_processes(size, chunk_size, params, folder, outputfile);
				});
				

			}
		});
	})
	.parse(process.argv);


