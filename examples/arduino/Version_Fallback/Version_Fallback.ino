// One sketch, two protocol versions.
//
// The same code talks to a broker installed last month and one installed in
// 2016. It asks for 5.0 and falls back to 3.1.1 only on evidence that the
// VERSION is the problem - never on a bad password, a TLS failure or a network
// error, because treating one of those as a version problem hides it.
//
// Written in the FULL FORM, where fallback is off by default and turned on
// explicitly. MqttDevice turns it ON by default - working against whatever
// broker is installed is the whole point of that class - and
// mqtt.only(MqttVersion::V5) turns it off again.
//

#include <WiFi.h>
#include <ModBeeMqtt.h>

WiFiClient plain;
MqttStreamTransport<WiFiClient> transport(plain, "192.168.1.10", 1883);
MqttClient mqtt;

void setup() {
  Serial.begin(115200);
  WiFi.begin("plant-wifi", "");
  while (WiFi.status() != WL_CONNECTED) delay(250);

  MqttConnectionConfig connection;
  connection.host = "192.168.1.10";
  connection.port = 1883;
  connection.clientId = "panel-3";
  connection.version = MqttVersion::V5;
  connection.fallbackEnabled = true;
  connection.fallbackVersion = MqttVersion::V311;

  mqtt.begin(transport, connection);
  mqtt.connect();
}

void loop() {
  mqtt.poll();

  // What is IN FORCE, which is not always what was asked for. Worth printing
  // once at commissioning: it is the difference between "the broker is old"
  // and "somebody changed the config".
  static bool reported = false;
  if (!reported && mqtt.isConnected()) {
    reported = true;
    Serial.print("connected using MQTT ");
    Serial.println(mq_versionName(mqtt.activeVersion()));
    Serial.print("keep alive in force: ");
    Serial.println(mqtt.activeKeepAlive());

    // 5.0 features are REFUSED on a 3.1.1 connection rather than silently
    // dropped, so this is worth asking before relying on one.
    Serial.print("maximum QoS the broker offers: ");
    Serial.println(mqtt.capabilities().maximumQos);
  }
}
