const fs = require("fs");
const RedisSMQ = require("rsmq");

const input = fs.readFileSync(0).toString();
const data = JSON.parse(input);

const rsmq = new RedisSMQ({host: data["host"], port: data["port"], ns: data["namespace"], realtime: data["real_time"]});

switch (data["method"]) {
case "send_message":
    rsmq.sendMessage({qname: data["qname"], message: data["message"], delay: data["delay"]}, function (err, resp) {
	    if (err) {
		    console.error(err);
		    process.exit(1);
	    }

	    console.log(JSON.stringify({"id": resp}));
        process.exit();
    });
    break;
case "create_queue":
    rsmq.createQueue({qname: data["qname"], vt: data["vt"], delay: data["delay"], maxsize: data["maxsize"]}, function (err, resp) {
	    if (err) {
		    console.error(err);
            process.exit(1);
	    }

	    console.log(JSON.stringify({}));
        process.exit();
    });
    break;
case "receive_message":
    rsmq.receiveMessage({qname: data["qname"], vt: data["vt"]}, function (err, resp) {
	    if (err) {
		    console.error(err);
            process.exit(1);
	    }

	    if (resp.id) {
		    console.log(JSON.stringify(resp));
	    } else {
		    console.log(JSON.stringify({}));
	    }

        process.exit();
    });
    break;
case "delete_message":
    rsmq.deleteMessage({ qname: data["qname"], id: data["id"] }, function (err, resp) {
	    if (err) {
		    console.error(err);
		    process.exit(1);
	    }

        console.log(JSON.stringify({"deleted": resp}));
        process.exit();
    });
    break;
default:
    throw new Error("Unknown method: " + data["method"]);
}
