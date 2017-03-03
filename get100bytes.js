const aws=require('aws-sdk')

s3 = new aws.S3('client')

var params = {
			Bucket: 'emr-workshop-maan',
			Key: 'input/scala.tgz',
			Range: 'bytes=0-100'
		}

s3.getObject(params, (err, data) => {
	if (err) throw err;
	console.log(data);
	console.log(data.Body.toString('hex'));
});