// A PLC reporting to a broker over TLS.
//
// Nothing here blocks. connect() queues the intent and returns; the connection
// completes across later calls to loop(). That is the whole reason this
// library exists rather than one of the others: a call that waits for a
// network round trip cannot live inside a scan cycle.
//
// Written in the FULL FORM. TLS_Over_Ethernet does the same job in the short
// form and over a wired link - TLS is a property of the network channel, not
// of this library, so WiFi is one option here and not the only one.
//

#include <WiFi.h>
#include <WiFiClientSecure.h>   // its own header since ESP32 core 3.x
#include <ModBeeMqtt.h>

// Not every board names its LED - a bare "ESP32 Dev Module" does not. GPIO 2
// is the usual one on those; change it to suit your board.
#ifndef LED_BUILTIN
#define LED_BUILTIN 2
#endif

const char* WIFI_SSID = "plant-wifi";
const char* WIFI_PASSWORD = "";

const char* BROKER = "broker.example.com";
const uint16_t BROKER_PORT = 8883;

// The authority that signed the broker's certificate. Without this the
// transport has nothing to verify against and refuses to connect - which is
// the right way round: a TLS client that accepts anything is an encrypted
// conversation with whoever answered.
const char* ROOT_CA = R"(-----BEGIN CERTIFICATE-----
...paste your CA here...
-----END CERTIFICATE-----)";

// WiFiClientSecure IS an Arduino network channel, so the same transport
// template covers TLS and plain TCP. TLS costs no extra code here at all.
WiFiClientSecure secure;
MqttStreamTransport<WiFiClientSecure> transport(secure, BROKER, BROKER_PORT);
MqttClient mqtt;

// --- Your variables ---
float tankLevel = 0.0f;
bool pumpRunning = false;

MqttTopicHandle levelTopic;
MqttTopicHandle commandTopic;

void onMessage(void*, const MqttMessage& message) {
  // The spans point into the receive buffer and are valid only for this call.
  // Anything that must outlive it is copied here, deliberately.
  if (message.payload.length == 1) {
    pumpRunning = message.payload.data[0] != '0';
  }
}

void setup() {
  Serial.begin(115200);

  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) delay(250);

  secure.setCACert(ROOT_CA);

  MqttConnectionConfig connection;
  connection.host = BROKER;
  connection.port = BROKER_PORT;
  connection.clientId = "tank-01";
  connection.version = MqttVersion::V5;

  // Talks to an old broker too. Off by default, because a plant engineer
  // should be able to read the config and know what is on the wire.
  connection.fallbackEnabled = true;
  connection.fallbackVersion = MqttVersion::V311;

  connection.keepAlive = 60;
  connection.cleanStart = false;
  connection.hasSessionExpiry = true;
  connection.sessionExpiryInterval = 3600;   // survive an hour of outage

  // A Will, so the broker announces us offline if we die without saying
  // goodbye. The payload is bytes, not text, because a Will may be anything.
  connection.will.set("plant/tank-01/status", "offline", 1, true);

  mqtt.begin(transport, connection);
  mqtt.onMessage(&onMessage, nullptr);

  levelTopic = mqtt.registerTopic("plant/tank-01/level");
  commandTopic = mqtt.registerTopic("plant/tank-01/command");

  // Subscribing before the connection exists is recorded, not refused. It goes
  // out when the session is established.
  mqtt.subscribeTo(commandTopic, 1);

  mqtt.connect();          // queues; returns immediately
}

void loop() {
  mqtt.poll();             // the whole contract

  tankLevel = analogRead(A0) * (10.0f / 4095.0f);

  // Publish at most once a second. Writes to one topic supersede, so a value
  // changing faster than the link carries sends the value it settled on
  // rather than a history of everything it passed through.
  static unsigned long lastSent = 0;
  if (millis() - lastSent >= 1000) {
    lastSent = millis();
    char reading[16];
    snprintf(reading, sizeof reading, "%.2f", tankLevel);
    mqtt.publishTo(levelTopic, (const uint8_t*)reading, strlen(reading), 1, false);
  }

  digitalWrite(LED_BUILTIN, pumpRunning ? HIGH : LOW);
}
