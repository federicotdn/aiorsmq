const fs = require("fs");
const RedisSMQ = require("rsmq");

const input = fs.readFileSync(0).toString();
const data = JSON.parse(input);

const rsmq = new RedisSMQ({host: data["host"], port: data["port"], ns: data["namespace"], realtime: data["real_time"]});

switch (data["method"]) {
case "send_message":
    rsmq.sendMessage({qname: data["qname"], message: data["message"], delay: data["delay"]}, function (err, resp) {
	    if (err) {
		    console.error(err)
		    process.exit(1)
	    }

	    console.log(JSON.stringify({"id": resp}));
        process.exit();
    });
    break;
case "create_queue":
    break;
default:
    throw new Error("Unknown method.");
}
