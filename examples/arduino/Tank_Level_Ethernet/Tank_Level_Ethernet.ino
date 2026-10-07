// A TANK LEVEL ON A DASHBOARD - over WIRED ETHERNET
//
// The job: a 4-20 mA level transmitter on a tank, read by this board, shown on
// a screen in the control room and logged by whatever the site uses. If the
// board or the cable fails, the screen must show that rather than the last
// number it happened to see.
//
// Why Ethernet: a panel in a plant room is wired far more often than it is on
// WiFi. EthernetClient is an Arduino network channel like any other, so the
// library is exactly the same - only these two lines differ from the WiFi
// examples, and nothing below them changes at all.
//
// Notice how little of this sketch is about MQTT. The deadband, the heartbeat,
// the online/offline pair and the reconnect are all the library's - which is
// the point.

#include <Ethernet.h>
#include <ModBeeMqtt.h>

byte MAC[] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xFE, 0x01 };
IPAddress STATIC_IP(192, 168, 1, 51);       // plant networks rarely use DHCP
const char* BROKER = "192.168.1.10";

EthernetClient net;
MqttDevice mqtt(net);

// The variable the dashboard is actually watching. The library publishes it;
// this sketch only has to keep it true.
float tankLevel = 0.0f;

float readLevel() {
  // 4-20 mA across a 165 ohm resistor into a 12-bit input, 0-10 m range.
  const float counts = (float)analogRead(A0);
  const float milliamps = counts * (25.0f / 4095.0f);
  const float fraction = (milliamps - 4.0f) / 16.0f;
  if (fraction < 0.0f) return 0.0f;           // below 4 mA: sensor or wiring
  if (fraction > 1.0f) return 10.0f;
  return fraction * 10.0f;
}

void setup() {
  Serial.begin(115200);

  // Ethernet.begin() blocks while it brings the link up. That is the network
  // library's doing, not this one's - which is why it is HERE, in setup(),
  // and not in the loop.
  Ethernet.begin(MAC, STATIC_IP);

  if (Ethernet.hardwareStatus() == EthernetNoHardware) {
    Serial.println("no ethernet shield found");
  }

  // "online" now, "offline" if this panel ever stops without saying goodbye.
  // Both retained, so a screen opened at 3 a.m. still learns the state.
  mqtt.statusTopic("plant/tank-01/status");

  // THE WHOLE OF THE REPORTING, IN ONE LINE.
  //
  // Publish when the level moves 20 mm, and at least once every 30 seconds
  // regardless. Both numbers matter and they are DIFFERENT decisions:
  //
  //   - the deadband stops a noisy 4-20 mA input publishing every scan, because
  //     the last digit of a real sensor never sits still
  //   - the heartbeat is what lets the dashboard tell a steady tank from a dead
  //     transmitter, which it cannot do at all without one
  //
  // Retained by default, because a level is a state rather than an event.
  mqtt.report("plant/tank-01/level", &tankLevel, 0.02, 30000);

  // A session, so a dropped link is a pause rather than a loss.
  mqtt.rememberSession(3600);

  mqtt.connect(BROKER, "tank-01");
}

void loop() {
  mqtt.poll();              // publishes the level when it is worth publishing

  Ethernet.maintain();      // renews a DHCP lease; harmless on a static address

  tankLevel = readLevel();

  // Once a minute, say what is actually happening on the link. On a
  // commissioning day this is the difference between "it does not work" and
  // knowing which half does not.
  static unsigned long lastReport = 0;
  if (millis() - lastReport >= 60000) {
    lastReport = millis();
    Serial.print("mqtt: ");
    Serial.print(mqtt.stateText());
    Serial.print("  waiting to send: ");
    Serial.println((unsigned long)mqtt.queued());
  }
}
