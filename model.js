// ============================================================
// LAB 4 — VARIANT 8
// Object: Pressure maintenance station
// Node.js model + Modbus TCP + MQTT + HTTP status
//
// Modbus TCP: 127.0.0.1:1502, Unit ID: 1
// HTTP:       http://127.0.0.1:4004/status
// MQTT:       mqtt://127.0.0.1:1883
// ============================================================

const ModbusRTU = require("modbus-serial");
const mqtt = require("mqtt");
const http = require("http");

// -------------------- SETTINGS --------------------

const MODBUS_HOST = process.env.MODBUS_HOST || "127.0.0.1";
const MODBUS_PORT = 1502;
const UNIT_ID = 1;

const HTTP_HOST = process.env.HTTP_HOST || "127.0.0.1";
const HTTP_PORT = 4004;

const MQTT_URL = process.env.MQTT_URL || "mqtt://127.0.0.1:1883";
const MQTT_TOPIC = "lab4/variant08/level";

// Variant 8 settings
const PRESSURE_MIN = 0;
const PRESSURE_MAX = 1000;
const PUMP_START_PRESSURE = 300;
const PUMP_STOP_PRESSURE = 500;
const LEVEL_BLOCK_LIMIT = 20;
const PRESSURE_ALARM_LIMIT = 700;

// -------------------- PROCESS STATE --------------------

const state = {
    pressure: 250,
    level: 60,

    autoMode: true,
    pumpCommand: false,
    pumpRunning: false,
    pumpBlocked: false,

    alarmHighPressure: false,

    mqttConnected: false,
    lastMqttMessage: null,
    lastUpdate: new Date().toISOString()
};

// -------------------- CONTROL LOGIC --------------------

function updateProcess() {
    // Check the low-level interlock
    state.pumpBlocked = state.level < LEVEL_BLOCK_LIMIT;

    // Automatic pump control with hysteresis
    if (state.autoMode) {
        if (state.pressure <= PUMP_START_PRESSURE) {
            state.pumpCommand = true;
        }

        if (state.pressure >= PUMP_STOP_PRESSURE) {
            state.pumpCommand = false;
        }
    }

    // Low level always blocks the pump
    if (state.pumpBlocked) {
        state.pumpCommand = false;
    }

    state.pumpRunning = state.pumpCommand && !state.pumpBlocked;

    // Simulate pressure change
    if (state.pumpRunning) {
        state.pressure += 8;
    } else {
        state.pressure -= 1;
    }

    // Keep pressure within the configured range
    state.pressure = Math.max(
        PRESSURE_MIN,
        Math.min(PRESSURE_MAX, state.pressure)
    );

    // High pressure alarm
    state.alarmHighPressure = state.pressure > PRESSURE_ALARM_LIMIT;

    state.lastUpdate = new Date().toISOString();
}

// -------------------- MODBUS REGISTER MAP --------------------
//
// This is the custom register map for our Variant 8 model.
// Float values use two 16-bit Holding Registers, big-endian.
//
// Holding Registers:
// 0-1: Pressure, float, kPa
// 2-3: Level, float, %
// 4-5: Pump start pressure, float, kPa
// 6-7: Pump stop pressure, float, kPa
// 8-9: High pressure alarm limit, float, kPa
//
// Coils:
// 0: Automatic mode
// 1: Pump command
// 2: Pump running
// 3: Pump blocked
// 4: High pressure alarm
// --------------------

function floatToRegisters(value) {
    const buffer = Buffer.alloc(4);
    buffer.writeFloatBE(value, 0);

    return [
        buffer.readUInt16BE(0),
        buffer.readUInt16BE(2)
    ];
}

function getRegisterValue(address) {
    const values = {
        0: state.pressure,
        2: state.level,
        4: PUMP_START_PRESSURE,
        6: PUMP_STOP_PRESSURE,
        8: PRESSURE_ALARM_LIMIT
    };

    for (const startAddress of Object.keys(values)) {
        const start = Number(startAddress);

        if (address === start || address === start + 1) {
            const registers = floatToRegisters(values[start]);

            return registers[address - start];
        }
    }

    return 0;
}

const modbusVector = {
    getHoldingRegister(address, unitID) {
        return getRegisterValue(address);
    },

    getCoil(address, unitID) {
        switch (address) {
            case 0: return state.autoMode;
            case 1: return state.pumpCommand;
            case 2: return state.pumpRunning;
            case 3: return state.pumpBlocked;
            case 4: return state.alarmHighPressure;
            default: return false;
        }
    },

    setCoil(address, value, unitID) {
        if (address === 0) {
            state.autoMode = Boolean(value);
            console.log("Modbus: automatic mode =", state.autoMode);
        }

        if (address === 1 && !state.autoMode) {
            state.pumpCommand = Boolean(value);
            console.log("Modbus: manual pump command =", state.pumpCommand);
        }
    }
};

// -------------------- START MODBUS TCP SERVER --------------------

const modbusServer = new ModbusRTU.ServerTCP(modbusVector, {
    host: MODBUS_HOST,
    port: MODBUS_PORT,
    debug: false,
    unitID: UNIT_ID
});

modbusServer.on("socketError", (error) => {
    console.error("Modbus socket error:", error.message);
});

modbusServer.on("error", (error) => {
    console.error("Modbus server error:", error.message);
});

console.log(
    `Modbus TCP server configured at ${MODBUS_HOST}:${MODBUS_PORT}, Unit ID ${UNIT_ID}`
);

// -------------------- MQTT CLIENT --------------------

const mqttClient = mqtt.connect(MQTT_URL, {
    connectTimeout: 3000,
    reconnectPeriod: 5000
});

mqttClient.on("connect", () => {
    state.mqttConnected = true;
    console.log("MQTT connected:", MQTT_URL);

    mqttClient.subscribe(MQTT_TOPIC, (error) => {
        if (error) {
            console.error("MQTT subscribe error:", error.message);
        } else {
            console.log("MQTT subscribed to:", MQTT_TOPIC);
        }
    });
});

mqttClient.on("reconnect", () => {
    console.log("MQTT reconnecting...");
});

mqttClient.on("close", () => {
    state.mqttConnected = false;
});

mqttClient.on("error", (error) => {
    state.mqttConnected = false;
    console.log("MQTT connection unavailable:", error.message);
});

mqttClient.on("message", (topic, message) => {
    try {
        const payload = message.toString();
        let receivedLevel;

        // Accept either a plain number, e.g. 45.5,
        // or JSON, e.g. {"level":45.5}
        try {
            const parsed = JSON.parse(payload);

            if (typeof parsed === "number") {
                receivedLevel = parsed;
            } else {
                receivedLevel = Number(parsed.level);
            }
        } catch {
            receivedLevel = Number(payload);
        }

        if (
            Number.isFinite(receivedLevel) &&
            receivedLevel >= 0 &&
            receivedLevel <= 100
        ) {
            state.level = receivedLevel;
            state.lastMqttMessage = new Date().toISOString();

            console.log("MQTT level received:", state.level, "%");
        } else {
            console.log("Ignored invalid MQTT level:", payload);
        }
    } catch (error) {
        console.error("MQTT message processing error:", error.message);
    }
});

// -------------------- HTTP STATUS AND TEST API --------------------

const httpServer = http.createServer((request, response) => {
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Content-Type", "application/json; charset=utf-8");

    if (request.method === "GET" && request.url === "/status") {
        response.writeHead(200);

        response.end(JSON.stringify({
            variant: 8,
            object: "Pressure maintenance station",
            communication: {
                modbus: `${MODBUS_HOST}:${MODBUS_PORT}`,
                unitId: UNIT_ID,
                mqttBroker: MQTT_URL,
                mqttTopic: MQTT_TOPIC,
                mqttConnected: state.mqttConnected
            },
            settings: {
                pumpStartPressure: PUMP_START_PRESSURE,
                pumpStopPressure: PUMP_STOP_PRESSURE,
                lowLevelBlock: LEVEL_BLOCK_LIMIT,
                highPressureAlarm: PRESSURE_ALARM_LIMIT
            },
            process: state
        }, null, 2));

        return;
    }

    // Test endpoint: change simulated pressure and/or level.
    // Example JSON: {"pressure":650,"level":55}
    if (request.method === "POST" && request.url === "/simulate") {
        let body = "";

        request.on("data", chunk => {
            body += chunk.toString();
        });

        request.on("end", () => {
            try {
                const data = JSON.parse(body);

                if (data.pressure !== undefined) {
                    const pressure = Number(data.pressure);

                    if (
                        !Number.isFinite(pressure) ||
                        pressure < PRESSURE_MIN ||
                        pressure > PRESSURE_MAX
                    ) {
                        throw new Error("Pressure must be between 0 and 1000 kPa");
                    }

                    state.pressure = pressure;
                }

                if (data.level !== undefined) {
                    const level = Number(data.level);

                    if (
                        !Number.isFinite(level) ||
                        level < 0 ||
                        level > 100
                    ) {
                        throw new Error("Level must be between 0 and 100%");
                    }

                    state.level = level;
                }

                updateProcess();

                response.writeHead(200);
                response.end(JSON.stringify({
                    message: "Simulation values updated",
                    process: state
                }, null, 2));
            } catch (error) {
                response.writeHead(400);
                response.end(JSON.stringify({
                    error: error.message
                }));
            }
        });

        return;
    }

    response.writeHead(404);
    response.end(JSON.stringify({
        error: "Not found",
        availableEndpoints: [
            "GET /status",
            "POST /simulate"
        ]
    }));
});

httpServer.listen(HTTP_PORT, HTTP_HOST, () => {
    console.log(`HTTP status server: http://${HTTP_HOST}:${HTTP_PORT}/status`);
});

// -------------------- PROCESS SIMULATION --------------------

console.log("Variant 8 pressure station model started.");
console.log("Initial pressure:", state.pressure, "kPa");
console.log("Initial level:", state.level, "%");

setInterval(() => {
    updateProcess();

    console.log(
        `Pressure: ${state.pressure.toFixed(1)} kPa | ` +
        `Level: ${state.level.toFixed(1)}% | ` +
        `Pump: ${state.pumpRunning ? "ON" : "OFF"} | ` +
        `Blocked: ${state.pumpBlocked ? "YES" : "NO"} | ` +
        `Alarm: ${state.alarmHighPressure ? "YES" : "NO"}`
    );
}, 1000);