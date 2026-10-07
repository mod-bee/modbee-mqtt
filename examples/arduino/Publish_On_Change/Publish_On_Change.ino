// Publishing a setpoint that moves faster than the link carries.
//
// The problem this solves: a control loop writing a setpoint every scan
// produces a hundred messages a second, of which ninety-nine are already
// stale by the time they reach the wire. Queueing them all wastes the link
// and delivers a history nobody wanted.
//
// Writes to one topic SUPERSEDE. The queued value is replaced rather than
// stacked behind, and the superseded caller is told - so what travels is the
// value the loop settled on.
//
// Written in the FULL FORM - MqttClient plus a transport - because what it
// shows is a queue behaviour, and the queue is the client's. For the short
// form, see First_Sketch and Tank_Level_Ethernet; MqttDevice supersedes the
// same way, because it is this client underneath.
//

#include <WiFi.h>
#include <ModBeeMqtt.h>

WiFiClient plain;
MqttStreamTransport<WiFiClient> transport(plain, "192.168.1.10", 1883);
MqttClient mqtt;

long superseded = 0;

void onPublished(void*, MqttResult result, uint16_t) {
  // Every accepted publish reaches exactly one conclusion, Superseded
  // included. A value that was replaced is reported rather than dropped
  // silently, because a lost setpoint looks exactly like a delivered one.
  if (result == MqttResult::Superseded) superseded++;
}

void setup() {
  Serial.begin(115200);
  WiFi.begin("plant-wifi", "");
  while (WiFi.status() != WL_CONNECTED) delay(250);

  MqttConnectionConfig connection;
  connection.host = "192.168.1.10";
  connection.port = 1883;
  connection.clientId = "loop-1";
  mqtt.begin(transport, connection);
  mqtt.connect();
}

void loop() {
  mqtt.poll();

  // Written every scan, deliberately. Most of these will be superseded before
  // they reach the wire, and that is the point.
  const float setpoint = analogRead(A0) * (100.0f / 4095.0f);
  char value[16];
  snprintf(value, sizeof value, "%.1f", setpoint);
  mqtt.publish("plant/loop-1/setpoint", value, 1, false, &onPublished, nullptr);

  static unsigned long lastReport = 0;
  if (millis() - lastReport >= 10000) {
    lastReport = millis();
    Serial.print("superseded before sending: ");
    Serial.println(superseded);
    Serial.print("actually published: ");
    Serial.println(mqtt.status().published);
  }
}
