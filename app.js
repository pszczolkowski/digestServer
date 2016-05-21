var async = require('async'),
	AWS = require("aws-sdk"),
	helpers = require("./helpers");

var AWS_CONFIG_FILE = "./config.json";
var APP_CONFIG_FILE = "./app.json";


AWS.config.loadFromPath(AWS_CONFIG_FILE);
var appConfig = helpers.readJSONFile(APP_CONFIG_FILE);

var s3 = new AWS.S3();
var simpledb = new AWS.SimpleDB();
var sqs = new AWS.SQS();

async.forever(main, handleError);


function main(next) {
	waitForMessages(next, function (messages) {
		var tasks = messages.map(msg => createTask(msg));

		async.series(tasks, function (err) {
			next(err);
		});
	});
}

function waitForMessages(next, callback) {
	var params = {
		QueueUrl: appConfig.QueueUrl,
		MaxNumberOfMessages: 1,
		VisibilityTimeout: 30,
		WaitTimeSeconds: 20
	};

	sqs.receiveMessage(params, function (err, data) {
		if (err || !data.Messages) {
			next(err);
		} else {
			callback(data.Messages);
		}
	});
}

function createTask(message) {
	return function (callback) {
		deleteMessage(message, function (err, data) {
			if (err) {
				callback(err);
				return;
			}

			var fileData = getFileDateFrom(message);

			s3.getObject(fileData, function (err, data) {
				if (err) {
					callback(err);
					return;
				}

				var loopCount = 1;
				helpers.calculateMultiDigest(
					data.Body.toString(),
					['md5', 'sha1', 'sha256', 'sha512'],
					onHashCalculated(fileData, callback),
					loopCount);
			});
		});
	};
}

function getFileDateFrom(message) {
	return JSON.parse(message.Body);
}

function deleteMessage(message, callback) {
	var params = {
		QueueUrl: appConfig.QueueUrl,
		ReceiptHandle: message.ReceiptHandle
	};

	sqs.deleteMessage(params, callback);
}

function onHashCalculated(fileData, callback) {
	return function (err, digests) {
		if (err) {
			callback(err);
			return;
		}

		var params = {
			Attributes: [{
				Name: 'md5',
				Value: digests[0],
				Replace: false
			}, {
				Name: 'sha1',
				Value: digests[1],
				Replace: false
			}, {
				Name: 'sha256',
				Value: digests[2],
				Replace: false
			}, {
				Name: 'sha512',
				Value: digests[3],
				Replace: false
			}],
			DomainName: 'pszczolkowski-file-digests',
			ItemName: fileData.Key
		};

		simpledb.putAttributes(params, function (err, data) {
			if (err) {
				callback(err);
			} else {
				console.log("Digest for file <" + fileData.Key + "> calculated");
				callback(null);
			}
		});
	}
}

function handleError(err) {
	console.log(err);
}
