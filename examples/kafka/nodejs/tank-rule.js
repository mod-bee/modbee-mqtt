// A small app on the modbee Kafka pipe.
//
// It reads every MQTT message from Kafka, prints it, and runs one rule: when
// the tank level goes above 8 m it tells the pump to stop, and below 2 m to
// start - by writing a Kafka record that modbee publishes to MQTT.
//
//   devices --MQTT--> modbee --> Kafka topic "mqtt-messages" --> this app
//   this app --> Kafka topic "mqtt-commands" --> modbee --MQTT--> devices
//
// Run it next to a docker/examples/*-kafka setup:
//   npm install
//   node tank-rule.js                  (on the same computer: localhost:9094)
//   KAFKA_BROKERS=kafka:9092 node ...  (in a container on the "modbee" network)
//
// docs/kafka-apps.md explains every line.

const { Kafka, logLevel } = require("@confluentinc/kafka-javascript").KafkaJS;

const brokers = (process.env.KAFKA_BROKERS || "localhost:9094").split(",");
const kafka = new Kafka({
  kafkaJS: { brokers, clientId: "tank-rule", logLevel: logLevel.WARN },
});

// The two Kafka topics the docker/examples/*-kafka modbee.yaml sets up.
const FROM_MQTT = "mqtt-messages";   // every MQTT message lands here
const TO_MQTT = "mqtt-commands";     // a record here is published to MQTT

const consumer = kafka.consumer({
  kafkaJS: {
    // Every copy of this app with the same groupId shares the work; a
    // different groupId gets its own copy of every message.
    groupId: "tank-rule",
    // The first time this group runs, start with what is already in Kafka.
    fromBeginning: true,
  },
});
const producer = kafka.producer();

// Sends one MQTT message: the record's KEY is the MQTT topic, its VALUE the
// payload (the mapping in modbee.yaml says mqtt-topic: "${KAFKA_KEY}").
async function publishToMqtt(mqttTopic, payload) {
  await producer.send({
    topic: TO_MQTT,
    messages: [{ key: mqttTopic, value: payload }],
  });
  console.log(`-> MQTT ${mqttTopic}: ${payload}`);
}

let pumpRunning = null;   // not known until we have decided once

async function onMqttMessage({ message }) {
  // The VALUE is the payload, byte for byte as the device sent it - text
  // here, but it could be JSON, or binary (Sparkplug B is protobuf). The
  // headers carry the rest of the MQTT message (headers: all), the topic
  // among them.
  const mqttTopic = message.headers["mqtt-topic"]?.toString();
  const payload = message.value ? message.value.toString() : "";
  const qos = message.headers["mqtt-qos"]?.toString();

  // The KEY is the device that published it: its MQTT client id, which is
  // also the mqtt-clientid header. Keying by device keeps each device's
  // messages in the order it sent them. A message with no key came from
  // Kafka - our own commands come back this way too, because EVERY MQTT
  // message goes to mqtt-messages, including the ones apps send.
  const from = message.key?.toString() || "a Kafka app";
  console.log(`<- MQTT ${mqttTopic}: ${payload}  (QoS ${qos}, from ${from})`);

  if (mqttTopic === "plant/tank-01/level") {
    // An empty payload is a retained message being cleared, not a reading:
    // Number("") is 0, which would read as an empty tank and start the pump.
    if (payload.trim() === "") return;
    const level = Number(payload);
    if (!Number.isFinite(level)) return;             // not a number: ignore it
    if (level > 8 && pumpRunning !== false) {
      await publishToMqtt("plant/pump-2/cmd", "stop");
      pumpRunning = false;
    } else if (level < 2 && pumpRunning !== true) {
      await publishToMqtt("plant/pump-2/cmd", "start");
      pumpRunning = true;
    }
  }
}

async function main() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topics: [FROM_MQTT] });
  console.log(`connected to Kafka at ${brokers.join(",")} - waiting for MQTT messages`);
  await consumer.run({ eachMessage: onMqttMessage });
}

// Ctrl-C (or docker stop): leave the group cleanly, so another copy of the
// app takes over at once rather than after a timeout.
async function stop() {
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
}
process.on("SIGINT", stop);
process.on("SIGTERM", stop);

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
