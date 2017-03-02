var cp = require('child_process');
var os = require('os');

var ncpus = os.cpus().length;
var d=Array()
for (var i=0; i<ncpus; i++) {
	var c = cp.fork(__dirname+"/child.js");
	d.push(c);
}

for (i in d) {
	d[i].send({type: 'bye', msg: 'hello'});
}

console.log('master_done')
setTimeout(function() {
	for (i in d) {
	d[i].send({type: 'end', msg: 'hello'});

}
console.log('Done for real')
}, 6000);