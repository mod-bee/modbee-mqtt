// A BOARD THAT IS THE BROKER.
//
// Not a client of one. This board accepts MQTT connections on 1883, routes
// between them, holds the retained values and publishes the Wills - so a plant
// panel can have a broker on it with no server anywhere on site and nothing to
// keep running in a cupboard.
//
// WHY PLAIN AND NOT TLS. Both exist; see Secure_Broker for the encrypted one.
// This is the ordinary case on an isolated line: twenty sensors on a machine,
// no route off it, and nothing between them that has a certificate store or a
// clock to check an expiry against. Encrypting a cable nobody else is on buys
// nothing and costs an engine, a second of key generation and a fingerprint
// somebody has to write down. Put the broker where the line is and bridge one
// connection upstream if head office needs the numbers - MqttBridge is that,
// and it is the connection that should be encrypted.
//
// ON THE NUMBERS. Everything this broker will ever use is allocated in
// begin(), from the config below, and never grows. That is what makes it
// usable on a board at all: the RAM it needs is knowable before it starts
// rather than a function of how busy the line gets.

#include <WiFi.h>
#include <ModBeeMqtt.h>

const char* WIFI_SSID = "plant-wifi";
const char* WIFI_PASSWORD = "";

const uint16_t MQTT_PORT = 1883;

WiFiServer listenSocket(MQTT_PORT);

// The two template arguments are the Arduino types this board has. On a wired
// panel - which is what most of them are - they are EthernetServer and
// EthernetClient instead, and nothing else in this sketch changes.
MqttStreamListener<WiFiServer, WiFiClient> listener(listenSocket);

MqttBroker broker;

void setup() {
    Serial.begin(115200);

    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    while (WiFi.status() != WL_CONNECTED) delay(250);
    Serial.print("ip ");
    Serial.println(WiFi.localIP());

    // Sized for what a panel actually holds, not for what a host can do.
    MqttBrokerConfig config;
    config.maxConnections = 4;
    config.maxSessions = 8;
    config.maxSubscriptionsPerSession = 8;
    config.maxQueuedPerSession = 16;
    config.receiveBufferSize = 1024;
    config.transmitBufferSize = 1024;
    config.maximumPacketSize = 1024;
    config.maxRetained = 32;

    broker.begin(config);

    listener.configure(config.maxConnections);
    if (!listener.begin()) {
        Serial.println("the listener would not start");
        return;
    }
    broker.addListener(listener);

    Serial.print("listening on ");
    Serial.println(MQTT_PORT);
}

void loop() {
    // The whole contract. It never waits for anything, which is why a PLC can
    // call it once per scan beside its ladder logic.
    broker.poll();

    // Worth printing while commissioning: TAKEOVERS climbing means two devices
    // were flashed with the same client identifier and are knocking each other
    // off for ever, each looking healthy on its own. This is the only place
    // that is visible.
    static uint32_t reportedAt = 0;
    if (millis() - reportedAt > 10000) {
        reportedAt = millis();
        const MqttBrokerStatus& status = broker.status();
        Serial.print("clients ");
        Serial.print((unsigned)broker.connectionCount());
        Serial.print("  in ");
        Serial.print((unsigned long)status.published);
        Serial.print("  out ");
        Serial.print((unsigned long)status.delivered);
        Serial.print("  dropped ");
        Serial.print((unsigned long)status.dropped);
        Serial.print("  takeovers ");
        Serial.println((unsigned long)status.takeovers);
    }
}
