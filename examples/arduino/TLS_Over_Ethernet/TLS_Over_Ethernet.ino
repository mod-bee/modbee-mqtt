// TLS OVER WIRED ETHERNET
//
// The job: the same tank level as the Ethernet example, but the broker is
// somewhere else - a cloud service, or across a site link that leaves the
// plant network - so the connection has to be encrypted and the broker's
// identity has to be checked.
//
// THE PROBLEM THIS SOLVES: most Ethernet chips an Arduino talks to - W5100,
// W5500, ENC28J60 - have no TLS of their own. Neither do most cellular modems
// in transparent mode. An ESP32's WiFiClientSecure does, which is why almost
// every MQTT example in the world uses WiFi for TLS and stops there.
//
// SSLClient (OPEnSLab) fixes that: it wraps ANY Arduino Client in TLS using
// BearSSL on the board itself. And because the result is itself an Arduino
// Client, this library needs no extra code at all - the transport template
// covers it exactly as it covers a plain EthernetClient.
//
// The same trick works over cellular: SSLClient over TinyGsmClient.
//
// WHAT IT COSTS, honestly: BearSSL wants roughly 40-50 kB of flash and enough
// RAM for its buffers. On an UNO R4 that is most of what is spare.
// An ESP32 with native TLS is cheaper - use that where you have one.

#include <Ethernet.h>
#include <SSLClient.h>
#include <ModBeeMqtt.h>

// Generated from your broker's CA with SSLClient's trust anchor tool, and
// #included like any other header. See SSLClient's own documentation.
#include "trust_anchors.h"

byte MAC[] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xFE, 0x02 };
const char* BROKER = "broker.example.com";

// The plain channel, and the TLS one wrapped around it. The analog pin gives
// BearSSL its entropy - it must be an UNCONNECTED one, or the randomness is
// not random.
EthernetClient plain;
SSLClient secure(plain, TAs, (size_t)TAs_NUM, A5);

// MqttDevice takes the SECURE one. Nothing else in the sketch differs from
// plain TCP, and the port is the only other change: 8883 rather than 1883.
MqttDevice mqtt(secure);

void setup() {
  Serial.begin(115200);
  Ethernet.begin(MAC);

  mqtt.statusTopic("plant/tank-01/status");
  mqtt.rememberSession(3600);

  // Brokers that require a certificate usually require a login as well.
  mqtt.credentials("tank-01", "the-password-from-your-secrets-file");

  mqtt.connect(BROKER, 8883, "tank-01");
}

void loop() {
  mqtt.poll();
  Ethernet.maintain();

  static unsigned long lastSent = 0;
  if (millis() - lastSent >= 30000) {
    lastSent = millis();
    const float level = analogRead(A0) * (10.0f / 4095.0f);
    mqtt.publish("plant/tank-01/level", level, MQ_QOS_1, true);
  }

  // A TLS handshake that fails looks exactly like a network fault from the
  // outside, so it is worth printing the reason once rather than guessing.
  static bool everConnected = false;
  if (mqtt.isConnected()) everConnected = true;
  if (!everConnected) {
    static unsigned long lastComplaint = 0;
    if (millis() - lastComplaint >= 10000) {
      lastComplaint = millis();
      Serial.print("not connected: ");
      Serial.print(mqtt.stateText());
      Serial.print(" / ");
      Serial.println(mqtt.lastErrorText());
    }
  }
}
