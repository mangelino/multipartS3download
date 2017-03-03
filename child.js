var fs = require('fs')

process.on('message', function(msg) {
	console.log(msg);
	if (msg.type === 'end') 
	{	
		var buf = new Buffer.from('ABC')
		msg.msg.content =  buf.toString('hex')
		process.send(msg.msg);
		process.stdout.write('ABCW');
	}
		
});

//console.log('Child here!')

