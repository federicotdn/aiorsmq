const RedisSMQ = require("rsmq");
const rsmq = new RedisSMQ({host: "localhost", port: 6379, ns: "rsmq"});

rsmq.popMessage({ qname: "myqueue" }, function (err, resp) {
	if (err) {
		console.error(err)
		return
	}

	if (resp.id) {
		console.log("Message received and deleted from queue", resp)
	} else {
		console.log("No messages for me...")
	}
});



// rsmq.sendMessage({ qname: "myqueue", message: "Hello World "}, function (err, resp) {
// 	if (err) {
// 		console.error(err)
// 		return
// 	}

// 	console.log("Message sent. ID:", resp);
// });
