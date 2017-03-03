var cp = require('child_process');
var os = require('os');
var fs = require('fs');


var ncpus = os.cpus().length;
fs.writeFile('test', new Buffer(ncpus*2*5), ()=>{})

fs.open('test','w', (err, fd) => {
	for (var i=0; i<ncpus; i++) {
		var c = cp.spawn('/usr/local/bin/node '+__dirname+"/child_swap.js");
		//c.send({type: 'end', msg : {'content': 'ABCD'.charAt(i), 'pos': i, 'fd':fd}});
		c.stdout.on('data', (data)=>{
			console.log(data)
			// fs.write(fd, Buffer.from(m.content, 'hex'), 0,3,m.pos*3*2, (err, w, s) => {
			// 	if (err) throw err; 
			// 	console.log("written "+w+":"+s)
			//})
		})
	}
})



console.log('master_done')
setTimeout(()=>{console.log('Wait done')}, 10000)
