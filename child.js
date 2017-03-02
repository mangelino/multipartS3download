process.on('message', function(msg) {
	console.log(msg);
	if (msg.type === 'end')
		process.exit(0);
});

console.log('Child here!')
//while (true)
setInterval(function() {console.log('Alive')}, 2000);
//;
