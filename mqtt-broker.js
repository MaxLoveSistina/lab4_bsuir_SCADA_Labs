const net = require("net");
const { Aedes } = require("aedes");

const HOST = process.env.HOST || "127.0.0.1";
const PORT = 1883;

async function startBroker() {
    const broker = await Aedes.createBroker();

    broker.on("client", (client) => {
        console.log("MQTT client connected:", client.id);
    });

    broker.on("clientDisconnect", (client) => {
        console.log("MQTT client disconnected:", client.id);
    });

    broker.on("publish", (packet, client) => {
        if (client) {
            console.log(
                "MQTT message:",
                packet.topic,
                packet.payload.toString()
            );
        }
    });

    const server = net.createServer((socket) => {
        broker.handle(socket);
    });

    server.on("error", (error) => {
        console.error("MQTT server error:", error.message);
    });

    server.listen(PORT, HOST, () => {
        console.log("MQTT broker started at mqtt://" + HOST + ":" + PORT);
    });
}

startBroker().catch((error) => {
    console.error("Broker startup failed:", error);
    process.exit(1);
});