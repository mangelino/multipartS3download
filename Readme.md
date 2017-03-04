# Multipart S3 download client
This is a client library to fetch S3 objects via range requests. 
This is typically an IO bound process, hence single threaded asynchronous calls are optimal for the task.
## Use
Clone the repository and run `npm install` to download the dependencies.
It has been tested with Node.js 7.1.1.
To use it, just run 

```bash
multipart.js <s3://bucket/key> <outputfile> [-s <size>] [-c <accessKey>/<secretAccessKey>]
```
The `<size>` optional parameter specifies the maximum chunk size in bytes that is fetched in parallel.

## Security

You must have the `~/.aws/credentials` and the `~/.aws/config` correctly setup or pass the `accessKey` and `secretAccessKey` as a parameter to the command.
