// Reading a value, and knowing whether to believe it.
//
// The problem this solves: a variable holding 21.5 says nothing about whether
// 21.5 is still true. On a plant where one sensor has quietly stopped
// publishing, its last reading looks identical to a healthy one - and MQTT
// makes this worse than most, because a retained message is served happily by
// a connected broker long after the device that wrote it has gone.
//
// So this tracks two things the value itself cannot say: how old it is, and
// whether the connection carrying it is up.
//
// Written in the FULL FORM, by hand, so the staleness logic is visible.
// MqttDevice::isFresh() does this for you, and Alarm_On_Silence shows what
// it is actually for.
//

#include <WiFi.h>
#include <ModBeeMqtt.h>

WiFiClient plain;
MqttStreamTransport<WiFiClient> transport(plain, "192.168.1.10", 1883);
MqttClient mqtt;

struct Reading {
  const char* name;
  MqttTopicHandle topic;
  float value;
  unsigned long receivedAt;
  bool everReceived;
};

Reading readings[] = {
  { "pressure", MQTT_TOPIC_HANDLE_NONE, 0.0f, 0, false },
  { "flow",     MQTT_TOPIC_HANDLE_NONE, 0.0f, 0, false },
};

// How long a reading may be silent before it stops being trusted. Stated by
// US, because only the application knows how often a value is expected -
// a temperature sampled hourly and a pressure sampled ten times a second
// cannot share a constant.
const unsigned long MAX_SILENCE_MS = 30000;

void setup() {
  Serial.begin(115200);
  WiFi.begin("plant-wifi", "");
  while (WiFi.status() != WL_CONNECTED) delay(250);

  MqttConnectionConfig connection;
  connection.host = "192.168.1.10";
  connection.port = 1883;
  connection.clientId = "hmi-1";
  mqtt.begin(transport, connection);

  readings[0].topic = mqtt.registerTopic("plant/line3/pressure");
  readings[1].topic = mqtt.registerTopic("plant/line3/flow");
  for (Reading& reading : readings) mqtt.subscribeTo(reading.topic, 1);

  mqtt.connect();
}

void loop() {
  mqtt.poll();

  for (Reading& reading : readings) {
    uint8_t buffer[32];
    size_t length = 0;
    if (mqtt.takeMessage(reading.topic, buffer, sizeof buffer - 1, length)) {
      buffer[length] = '\0';
      reading.value = atof((const char*)buffer);
      reading.receivedAt = millis();
      reading.everReceived = true;
    }
  }

  static unsigned long lastReport = 0;
  if (millis() - lastReport < 5000) return;
  lastReport = millis();

  for (const Reading& reading : readings) {
    Serial.print(reading.name);
    Serial.print(": ");
    if (!reading.everReceived) {
      // Never received is NOT the same as stale, and conflating them sends
      // somebody to the wrong end of the plant: one is a commissioning error,
      // the other is a cable.
      Serial.println("UNKNOWN - nothing has ever arrived on this topic");
    } else if (!mqtt.isConnected()) {
      Serial.println("BAD - the broker connection is down");
    } else if (millis() - reading.receivedAt > MAX_SILENCE_MS) {
      Serial.print("STALE - last heard ");
      Serial.print((millis() - reading.receivedAt) / 1000);
      Serial.println(" s ago");
    } else {
      Serial.println(reading.value);
    }
  }
}
