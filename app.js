var aws = require('aws-sdk');
var express  = require('express');
var http = require('http');
var app = express();
var queueUrl = "https://sqs.us-east-1.amazonaws.com/189843593504/anicaport_delete_dev";
var delQueryUrl = "https://sqs.us-east-1.amazonaws.com/189843593504/anicaport_status_dev";

// Load your AWS credentials and try to instantiate the object.
aws.config.loadFromPath(__dirname + '/config.json');

// Instantiate SQS.
var sqs = new aws.SQS();
var s3 = new aws.S3();
var ets = new aws.ElasticTranscoder();
var pipelineId = '1474890258149-m40gy6';

var interval = setInterval(function(){

	var queuedJobParams = {
    	QueueUrl: queueUrl,
    	VisibilityTimeout: 600 // 10 mins wait time for anyone else to process.
	};

	var completedJobParams = {
		QueueUrl: delQueryUrl,
    	VisibilityTimeout: 20
	};

  	sqs.receiveMessage(queuedJobParams, function(err, queuedJobs) {
	    if(err) {
	        // res.send(err);
	        console.log(err);
	    }
	    else {
	        if (queuedJobs.hasOwnProperty('Messages')){

	        	// cache queued messages
	        	var queuedMsgs = queuedJobs.Messages;


	        	// fetch data from completed jobs queue
	        	sqs.receiveMessage(completedJobParams, function(err, completedJobs) {
				    if(err) {
				        // res.send(err);
				        console.log(err);
				    }
				    else {

				    	// console.log("queuedJobs : "+JSON.stringify(queuedJobs));
				    	// console.log("completedJobs : "+JSON.stringify(completedJobs));


				    	var completedMsgs = completedJobs.Messages;
				    	for (var i = 0 ; i < queuedMsgs.length; i++) {
				    		var isVideoTranscoded = false;
				    		var queuedJsonBody = JSON.parse(queuedMsgs[i].Body);
				    		var queuedKey = queuedJsonBody.Records[i].s3.object.key;
				    		var bucketName = queuedJsonBody.Records[i].s3.bucket.name
	        				console.log('Queued Key : '+queuedKey);

	        				if (completedJobs.hasOwnProperty('Messages')) {

	        					for (var j = 0; j < completedMsgs.length; j++) {

		        					var completedJsonBody = JSON.parse(completedMsgs[j].Body);
					    			var completedKey = JSON.parse(completedJsonBody.Message).input.key;

							    	console.log(completedKey === queuedKey);
							    	if (completedKey === queuedKey) {
							    		isVideoTranscoded = true;

							    		console.log("Removing completed Jobs from queue : "+completedKey);

								    	// delele msg from queued queue and completed queue
								    	var queuedJobDeleteParams = {
									        QueueUrl: queueUrl,
									        ReceiptHandle: queuedMsgs[i].ReceiptHandle
									    };

									    console.log(queuedJobDeleteParams);
								    	sqs.deleteMessage(queuedJobDeleteParams, function(err, data) {
								        	if(err) {
								            	console.log(err);
								        	} else {
								        		console.log("Message Deleted");
								            	console.log(data);
								        	}
										});

										// // delele msg from queued queue and completed key
								    	var completedJobDeleteParams = {
									        QueueUrl: delQueryUrl,
									        ReceiptHandle: completedMsgs[j].ReceiptHandle
									    };
									    console.log(completedJobDeleteParams);
								    	sqs.deleteMessage(completedJobDeleteParams, function(err, data) {
								        	if(err) {
								            	console.log(err);
								        	} else {
								        		console.log("Message Deleted");
								            	console.log(data);
								        	}
										});
							    	}
		        				};

		        				if (!isVideoTranscoded) {
		        					// video is not transcoded in specified time so 
							    	// so submitting the request again
							    	var transcodeParams = {
							    		src: queuedKey,
							   			bucket: bucketName
							   		};
							    	console.log("video is not transcoded in specified time so submitting the request again "+transcodeParams);
		        				};
		        				
					    	} else {
					    		// completed msg queue is empty we can begin transcoding
					    		console.log("completed queue don't contain any messages");
					    		var transcodeParams = {
					    			src: queuedKey,
					    			bucket: bucketName
					    		};
					    		transcodeVideo(transcodeParams);
					    	}
	        				
	        			};
				    }
				});
	        };
	    };
	});
},2000);

function transcodeVideo(transcodeParams) {
  	console.log("Transcoding Started");
  	console.log("SRC "+transcodeParams.src);
    
    var srcKey = transcodeParams.src;
    var srcBucket = transcodeParams.bucket;


    // elastic transcoder logic
    var arr = srcKey.split("/");
    var prefixPath = arr[0]+"/"+arr[1]+"/"+arr[2];
    var uuid = arr[4];
    var file_name = arr[5].split(".")[0];
    var dstKey = prefixPath+"/destination/"+uuid+"/";
    var originKey = srcKey;

    // Sanity check: validate that source and destination are different buckets.
    if (srcKey == dstKey) {
      	console.error("Destination bucket must not match source bucket.");
      	return;
    }



    var filename1 = "240p/"+file_name+"_";
    var filename2 = "360p/"+file_name+"_";
    var filename3 = "480p/"+file_name+"_";
    var filename4 = "720p/"+file_name+"_";
    var filename5 = "1080p/"+file_name+"_";
    var playlist_name = file_name+"_master_playlist"


    s3.getObject({Bucket:srcBucket, Key:srcKey},
      	function(err,data) {
        	if (err) {
	           console.log('error getting object ' + srcKey + ' from bucket ' + srcBucket +
	               '. Make sure they exist and your bucket is in the same region as this function.');
	           console.log('error','error getting file'+err);
        	} else {
            	console.log("### JOB KEY ### " + data);

            	ets.createJob({PipelineId: pipelineId,
                	OutputKeyPrefix: dstKey,
                	Input: {
	                    Key: originKey,
	                    FrameRate: 'auto',
	                    Resolution: 'auto',
	                    AspectRatio: 'auto',
	                    Interlaced: 'auto',
	                    Container: 'auto'
                	},
	                Outputs: [{
	                    Key: filename5,
	                    PresetId: '1351620000001-000001',
	                    Rotate: 'auto',
	                }],

            	}, function(err, data) {
	                if (err){
	                  	console.log(err, err.stack);
	                  	console.log('Failed');// an error occurred
	                }
	                else {
                  
	  					console.log("Transcoding Job Submitted");
	                  	console.log(data);

	     //              // transcoding completed now we can delete msg from sqs
	     //              sqs.deleteMessage(params, function(err, data) {
				  //       	if(err) {
				  //           	console.log(err);
				  //       	} else {
				  //       		console.log("Message Deleted");
				  //           	console.log(data);
				  //       	}
						// });

                	}
            	});
        	}
      	}
    );
}

// clearInterval(interval);


app.use(express.static('public'));
var server = http.createServer(app);

app.get("/",function (req, res) {
	console.log("index hit");
	res.status(200).send('hello from transcoder');
});

// Start server.
server.listen('8081', "127.0.0.1");
console.log("server is running on http://%s:%s", "127.0.0.1", 8081);
module.exports = app;