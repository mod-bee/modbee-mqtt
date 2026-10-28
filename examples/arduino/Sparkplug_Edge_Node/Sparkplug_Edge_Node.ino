// A Sparkplug B edge node: a tank and its pump, for any Sparkplug SCADA
// system - Ignition, a ModBee host, anything conformant - to find by itself.
//
// What Sparkplug adds to plain MQTT topics:
//   - the node announces every metric it has, with its type and units, in a
//     BIRTH certificate, so the SCADA system builds its tag list from that
//     and nobody types tag names in by hand;
//   - the broker announces the node's DEATH when it drops off, so a stale
//     value is never mistaken for a live one;
//   - every message carries a sequence number, so a lost one is noticed and
//     the node is asked to re-announce itself.
//
// Two things it needs from you:
//   - THE TIME. Sparkplug stamps every message in UTC, and an ESP32 only knows
//     how long it has been running until NTP answers - so setup() waits for
//     the clock before connecting.
//   - EVERY METRIC DECLARED BEFORE connect(). The birth lists them all, and a
//     metric that was not in it cannot be published later.
//
// It appears in SCADA as group "Plant", node "Tank-01". docs/sparkplug.md has
// the rest, including TLS and failing over between brokers.

#include <WiFi.h>
#include <time.h>
#include <ModBeeMqtt.h>

const char* WIFI_NAME = "plant-wifi";
const char* WIFI_PASSWORD = "";
const char* BROKER = "192.168.1.10";

const uint8_t PUMP = 5;

WiFiClient net;
MqttSparkplugNode node(net);   // or EthernetClient, TinyGsmClient, WiFiClientSecure

float setpoint = 2.0f;   // metres - the SCADA system can change it

// A write from the SCADA system. Only a metric declared writable arrives here;
// a write to any other is refused, and counted in node.commandsRefused().
void onCommand(const MqttSparkplugCommand& command) {
  if (strcmp(command.name, "Tank/Setpoint") == 0) {
    setpoint = (float)command.value.asNumber();
  }
}

void setup() {
  Serial.begin(115200);
  pinMode(PUMP, OUTPUT);

  WiFi.begin(WIFI_NAME, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) delay(250);

  // UTC from NTP. Until it answers, time() counts up from 1970.
  configTime(0, 0, "pool.ntp.org");
  while (time(nullptr) < 1700000000) delay(250);
  node.epochNow((uint64_t)time(nullptr) * 1000);

  node.group("Plant");
  node.node("Tank-01");
  node.server(BROKER);   // port 1883; add more with node.addServer() to fail over

  node.declare("Tank/Level", MqttSparkplugDataType::Float, "m");
  node.declare("Pump/Running", MqttSparkplugDataType::Boolean);
  node.declareWritable("Tank/Setpoint", MqttSparkplugDataType::Float);
  node.onCommand(&onCommand);

  // A link that dies without saying so - WiFi out of range, a cellular modem
  // that has lost its bearer - swallows what is written into it until the
  // keepalive notices. Keep the last 64 values until the broker has proved
  // it has them, and send any it may not have again, as history, after the
  // next birth. 1.5 kB of heap; leave it out and nothing is kept.
  node.replayWindow(64);

  node.connect();   // returns at once; the birth goes when the link is up
}

void loop() {
  node.poll();

  const float level = analogRead(A0) * (5.0f / 4095.0f);
  const bool running = level < setpoint;
  digitalWrite(PUMP, running ? HIGH : LOW);

  node.set("Tank/Level", mq_spFloat(level));
  node.set("Pump/Running", mq_spBool(running));
  node.set("Tank/Setpoint", mq_spFloat(setpoint));

  // Once a second, one message carrying only what changed.
  static unsigned long lastSent = 0;
  if (millis() - lastSent >= 1000) {
    lastSent = millis();
    node.publishChanged();
  }

  static unsigned long lastReport = 0;
  if (millis() - lastReport >= 10000) {
    lastReport = millis();
    Serial.println(node.isBorn() ? "online in SCADA" :
                   node.isConnected() ? node.waitingFor() : node.lastErrorText());
  }
}
