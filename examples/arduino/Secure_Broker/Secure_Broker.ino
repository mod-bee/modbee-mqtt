// A BOARD THAT IS THE BROKER, OVER TLS.
//
// Not a client of one. This ESP32 accepts MQTT connections, encrypts them, and
// serves a certificate it made for itself on first start - so a panel can have
// a broker on it without a server anywhere, and without anything on the wire
// being readable.
//
// WHAT MAKES IT POSSIBLE. MqttBroker has never been host-only; what was
// missing was a listener that could accept a TLS connection without a BSD
// socket. MqttStreamTlsListener is that, and MqttIdentity mints the
// certificate with wolfSSL's wc_MakeCert - an EC P-256 key, about a second on
// an ESP32, where RSA-2048 would be minutes.
//
// BUILDING IT. Two things the Arduino tools need told:
//   - wolfSSL is only added to a build whose sketch includes one of its
//     headers, and ModBeeMqtt.h deliberately does not; include your wolfSSL
//     package's header ABOVE ModBeeMqtt.h.
//   - size: with ModBee's wolfSSL package (its board profile: TLS 1.2,
//     P-256, AES-128-GCM - see its user_settings.h) the whole ESP32 program
//     is about 1.13 MB, inside the default 1.3 MB app partition. A stock
//     wolfSSL build is larger (about 1.33 MB here) and needs Tools >
//     Partition Scheme > "Huge APP".
//
// COMMISSIONING. Print the fingerprint once and give it to whatever connects:
// the clients pin it, and pinning does not expire, so there is no certificate
// to renew on a device with no clock and no way to reach Let's Encrypt.

#include <WiFi.h>
#include <ModBeeMqtt.h>

// SERVING TLS NEEDS AN ENGINE ON THE BOARD, and that means the wolfSSL library
// installed alongside this one. Without it there is nothing to make a
// certificate with or encrypt through, so the sketch says so in one line here
// rather than in fifty lines of errors further down.
#if !MQTT_TLS_AVAILABLE
#warning "Secure_Broker needs wolfSSL installed: no TLS engine was found, so this sketch does nothing."
void setup() {}
void loop() {}
#else

const char* WIFI_SSID = "plant-wifi";
const char* WIFI_PASSWORD = "";

const uint16_t MQTT_TLS_PORT = 8883;

WiFiServer listenSocket(MQTT_TLS_PORT);

// The two template arguments are the Arduino types this board has. On a wired
// panel they are EthernetServer and EthernetClient instead, and nothing else
// in this sketch changes.
MqttStreamTlsListener<WiFiServer, WiFiClient> listener(listenSocket);

MqttBroker broker;

void setup() {
    Serial.begin(115200);

    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    while (WiFi.status() != WL_CONNECTED) delay(250);
    Serial.print("ip ");
    Serial.println(WiFi.localIP());

    // Sized for what a panel actually holds. Everything is allocated here and
    // never grows, so the memory this broker uses is knowable before it starts.
    MqttBrokerConfig config;
    config.maxConnections = 4;
    config.maxSessions = 8;
    config.maxSubscriptionsPerSession = 8;
    config.maxQueuedPerSession = 16;
    config.receiveBufferSize = 1024;
    config.transmitBufferSize = 1024;
    config.maximumPacketSize = 1024;

    broker.begin(config);

    listener.configure(config.maxConnections);
    if (!listener.begin()) {
        Serial.print("the listener would not start: ");
        Serial.println(listener.lastError().c_str());
        return;
    }
    broker.addListener(listener);

    // WRITE THIS DOWN. It is what a client pins, and it does not change unless
    // the key does - so a certificate regenerated from the same key keeps it.
    Serial.print("broker fingerprint ");
    Serial.println(listener.fingerprint().c_str());
}

void loop() {
    // The whole contract. It never waits for anything, which is why a PLC can
    // call it once per scan beside its ladder logic.
    broker.poll();
}

#endif  // MQTT_TLS_AVAILABLE
