const amqp = require("amqplib/callback_api");

const args = process.argv.slice(2);

if (args.length == 0) {
    console.log("Usage: rpc_client.js num");
    process.exit(1);
}

async function getConnection() {
    return new Promise((resolve, reject) => {
        amqp.connect("amqp://localhost:5672", (err, connection) => {
            if (err) {
                reject(err);
            }
            resolve(connection);
        });
    });
}

async function createChannel(connection) {
    return new Promise((resolve, reject) => {
        connection.createChannel((err, channel) => {
            if (err) {
                reject(err);
            }
            resolve(channel);
        });
    });
}

async function getQueue(channel, queueName = "") {
    return new Promise((resolve, reject) => {
        channel.assertQueue(
            queueName,
            {
                exclusive: true,
            },
            (error2, q) => {
                if (error2) {
                    reject(error2);
                }
                resolve(q);
            }
        );
    });
}

function generateUuid() {
    return (
        Math.random().toString() +
        Math.random().toString() +
        Math.random().toString()
    );
}

async function callRPC(channel, q, num) {
    let startMs = Date.now();
    return new Promise((resolve, reject) => {
        let correlationId = generateUuid();

        console.log(" [x] Requesting fib(%d)", num);

        channel.consume(
            q.queue,
            (msg) => {
                console.log("msgReceived", msg.properties.correlationId);
                if (msg.properties.correlationId === correlationId) {
                    console.log(`took: ${Date.now() - startMs}ms`);
                    resolve(msg);
                    // setTimeout(function () {
                    //     connection.close();
                    //     process.exit(0);
                    // }, 500);
                } else {
                    reject({
                        msg: `msg correlation id: ${msg.properties.correlationId} != ${correlationId}`,
                    });
                }
            },
            {
                noAck: true,
            }
        );

        channel.sendToQueue("rpc_queue", Buffer.from(num.toString()), {
            correlationId: correlationId,
            replyTo: q.queue,
        });
    });
}

async function run(args) {
    try {
        let connection = await getConnection();
        console.log("connection");
        let channel = await createChannel(connection);
        console.log("channel");
        let q = await getQueue(channel, "");
        console.log("q");

        let arraySize = parseInt(args[0]) | 10;

        for (let i = 1; i <= arraySize; i++) {
            console.log(`\n\ncall ${i} of ${arraySize}`);
            const msg = await callRPC(channel, q, 3);
            console.log(" [.] Got %s", msg.content.toString());
        }
    } catch (e) {
        console.error("catch", e);
    }
}

run(args);
