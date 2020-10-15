const amqp = require("amqplib/callback_api");

const args = process.argv.slice(2);

if (args.length == 0) {
    console.log("Usage: rpc_client.js num");
    process.exit(1);
}

class RPCClient {

    constructor(){
        this._connectionPromise;
        this._channelPromise;
        this._qPromise;
    
        this._callbackPromises = {};
    }
    

    async start() {
        let connection = await this.getConnection();
        let channel = await this.createChannel(connection);
        let q = await this.getQueue(channel, "");
        this.setupConsumer(channel,q, this._callbackPromises);
    }

    async getConnection() {
        if (!this._connectionPromise) {
            this._connectionPromise = new Promise((resolve, reject) => {
                amqp.connect("amqp://localhost:5672", (err, connection) => {
                    if (err) {
                        reject(err);
                    }
                    resolve(connection);
                });
            });
        }
        return this._connectionPromise;

    }

    async createChannel(connection) {
        if (!this._channelPromise) {
            this._channelPromise = new Promise((resolve, reject) => {
                connection.createChannel((err, channel) => {
                    if (err) {
                        reject(err);
                    }
                    resolve(channel);
                });
            });
        }
        return this._channelPromise;
    }

    async getQueue(channel, queueName = "") {
        if (!this._qPromise) {
            this._qPromise = new Promise((resolve, reject) => {
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
        return this._qPromise;
    }

    setupConsumer(channel, q, map) {
        channel.consume(
            q.queue,
            (msg) => {
                const resolveFn = map[msg.properties.correlationId];
                if (resolveFn) {
                    resolveFn(msg);
                } else {
                    console.log(`Promise not found for: ${msg.properties.correlationId}`);
                }
            },
            {
                noAck: true,
            }
        );
    }

    async callRPC(num) {
        const channel = await this._channelPromise;
        const q = await this._qPromise;

        let startMs = Date.now();//perf
        let correlationId = this.generateUuid();

        let resolveFn;
        const result = new Promise(resolve => {
            resolveFn = resolve;
        }).then(data => {//perf
            console.log(`took: ${Date.now() - startMs}ms`);
            return data;
        });

        this._callbackPromises[correlationId] = resolveFn;

        channel.sendToQueue("rpc_queue", Buffer.from(num.toString()), {
            correlationId: correlationId,
            replyTo: q.queue,
        });

        return result;
    }

    generateUuid() {
        return (
            Math.random().toString() +
            Math.random().toString() +
            Math.random().toString()
        );
    }
}


async function run(args) {
    try {
        let arraySize = parseInt(args[0]) || 100;

        const client = new RPCClient();
        await client.start();

        for (let i = 1; i <= arraySize; i++) {
            console.log(`\n\ncall ${i} of ${arraySize}`);
            const msg = await client.callRPC(3);
            console.log(" [.] Got %s", msg.content.toString());
        }
        process.exit();
    } catch (e) {
        console.error("catch", e);
    }
}

run(args);
