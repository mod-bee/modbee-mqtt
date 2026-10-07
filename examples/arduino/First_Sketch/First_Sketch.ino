// YOUR FIRST MQTT SKETCH
//
// A sensor that publishes a reading, and a light that somebody else can turn
// on. That is the whole of it, and it is enough to be genuinely useful.
//
// If MQTT is new to you, docs/getting-started.md explains what a broker is and
// what publish and subscribe mean, in plain words, before any code.
//
// What you need: a broker somewhere on the network. Mosquitto on a Raspberry
// Pi, an IoT gateway, or a cloud service - they all speak the same protocol.
// Put its address in BROKER below.

#include <WiFi.h>
#include <ModBeeMqtt.h>

// Not every board names its LED - a bare "ESP32 Dev Module" does not. GPIO 2
// is the usual one on those; change it to suit your board.
#ifndef LED_BUILTIN
#define LED_BUILTIN 2
#endif

const char* WIFI_SSID     = "plant-wifi";
const char* WIFI_PASSWORD = "";
const char* BROKER        = "192.168.1.10";

// The network channel is the sketch's, because the sketch is what starts WiFi.
// MqttDevice borrows it - one object, and the rest is done for you.
WiFiClient net;
MqttDevice mqtt(net);

// Called whenever anybody publishes to this topic. You get the topic that
// actually arrived and the payload as plain text.
void onLight(const char* topic, const MqttPayload& payload) {
  (void)topic;
  digitalWrite(LED_BUILTIN, payload.toBool() ? HIGH : LOW);
}

void setup() {
  Serial.begin(115200);
  pinMode(LED_BUILTIN, OUTPUT);

  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) delay(250);

  // "online" now, "offline" if this board ever stops without saying goodbye.
  // Both retained, so a dashboard that connects tomorrow still knows.
  mqtt.statusTopic("demo/board-1/status");

  // Subscribes AND registers the handler. One call, because you never want
  // one without the other.
  mqtt.onMessage("demo/board-1/light", &onLight);

  // Returns straight away. The connection completes in the background, across
  // later calls to poll() - so there is nothing here to wait for.
  mqtt.connect(BROKER, "board-1");
}

void loop() {
  mqtt.poll();          // the whole contract: call it every time round

  // Once a second, say what the sensor reads. Publishing while the link is
  // down is held and sent when it comes back, so there is no need to check.
  static unsigned long lastSent = 0;
  if (millis() - lastSent >= 1000) {
    lastSent = millis();
    mqtt.publish("demo/board-1/reading", analogRead(A0));
  }
}
