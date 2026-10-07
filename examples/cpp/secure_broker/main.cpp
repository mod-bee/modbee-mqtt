// A broker that serves TLS with a certificate it made itself.
//
//     modbee_secure_broker [port]
//     modbee_secure_broker 8883
//
// It prints a fingerprint on the first line. That is the whole configuration:
//
//     modbee-mqtt: pin this -> 23c3ca67edd89db8...
//
// Point a client at it with that number and nothing else - no CA, no
// certificate to install, no authority to ask:
//
//     MqttTlsTransport transport;
//     transport.configure("10.0.0.10", 8883);
//     transport.setPinnedKeyFingerprint("23c3ca67edd89db8...");
//
// Or verify it the ordinary way, because the certificate is its own trust
// anchor (CA:TRUE):
//
//     mosquitto_sub -h localhost -p 8883 --cafile broker.crt --insecure
//                   -t 'plant/#' -v
//
// WHAT THIS IS FOR. A plant panel where nobody is going to renew a
// certificate. The broker mints one on its first start, keeps it beside the
// program, and presents the same one every start after - so the number written
// on the panel at commissioning stays true for the life of the device.
//
// Self-signed here is the zero-configuration DEFAULT, not a ceiling: hand
// useCertificateFile() a real certificate from the site's own CA and clients
// verify it the ordinary way instead. See docs/security.md.

#include <ModBeeMqtt.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>

#if !MQTT_TLS_SOCKETS

int main() {
    std::printf("this build has no TLS engine - install wolfSSL or OpenSSL\n");
    return 1;
}

#else

namespace {
volatile std::sig_atomic_t g_stop = 0;
void onSignal(int) { g_stop = 1; }
}  // namespace

int main(int argc, char** argv) {
    const uint16_t port = (uint16_t)(argc > 1 ? std::atoi(argv[1]) : 8883);

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    // The identity. loadOrGenerate mints one the first time and finds the same
    // one every time after, which is what makes the fingerprint worth writing
    // down.
    MqttIdentity identity;
    identity.setCommonName("modbee-broker");
    identity.setSubjectAltNames("DNS:modbee-broker,DNS:localhost,IP:127.0.0.1");
    if (!identity.loadOrGenerate("broker.crt", "broker.key")) {
        std::printf("no identity: %s\n", identity.lastError().c_str());
        return 1;
    }

    MqttTlsListener secure;
    secure.configure("0.0.0.0", port, 32);
    secure.useCertificate(identity.certificatePem().c_str(),
                          identity.privateKeyPem().c_str());
    if (!secure.begin()) {
        // An operator-facing sentence, not a code: "port 8883 is already in
        // use", "the private key does not match the certificate".
        std::printf("not listening: %s\n", secure.lastError().c_str());
        return 1;
    }

    MqttBroker broker;
    broker.begin();
    if (!broker.addListener(secure)) {
        std::printf("the broker would not take the listener\n");
        return 1;
    }

    std::printf("modbee-mqtt: serving TLS on %u\n", (unsigned)secure.boundPort());
    std::printf("modbee-mqtt: pin this -> %s\n", identity.fingerprint().c_str());
    std::fflush(stdout);

    // One loop, and nothing in it waits. poll() services the accept, every
    // handshake in progress and every connected session.
    while (!g_stop) {
        broker.poll();
        mq_yield();
    }

    std::printf("\nstopping\n");
    broker.end();
    secure.end();
    return 0;
}

#endif  // MQTT_TLS_SOCKETS
