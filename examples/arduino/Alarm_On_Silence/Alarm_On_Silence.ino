// A PANEL THAT RAISES AN ALARM WHEN A DEVICE GOES QUIET
//
// The job: a small supervisory panel watching several field devices. If one of
// them stops reporting, a light comes on and a message goes out. This is the
// other half of every statusTopic() in the other examples - the half that
// actually does something with them.
//
// THE THING MQTT MAKES EASY TO GET WRONG:
//
//   A connected broker will happily hand you a retained reading from a device
//   that died last Tuesday, and say nothing about it. "I have a value" and
//   "this value is current" are different facts, and only one of them is safe
//   to act on.
//
//   So this panel judges on TWO signals and needs both:
//     - the device's own status topic, which its Will sets to "offline" when
//       it stops without saying goodbye
//     - silence: no reading within the interval the device promised
//
//   The Will alone is not enough. It fires when a TCP connection drops, which
//   does not happen if the device is wedged, or if its network card is alive
//   while its program is not. Silence catches that; the Will catches the rest.

#include <Ethernet.h>
#include <ModBeeMqtt.h>

byte MAC[] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xFE, 0x99 };
IPAddress STATIC_IP(192, 168, 1, 60);
const char* BROKER = "192.168.1.10";

const uint8_t PIN_ALARM_LAMP = 7;

EthernetClient net;
MqttDevice mqtt(net);

// What is being watched, and how often each device promised to speak. The
// interval is PER DEVICE, because a tank sampled every thirty seconds and a
// weather station sampled every ten minutes cannot share a constant.
struct Watched {
    const char* name;
    unsigned long expectedEveryMs;
    unsigned long lastHeardAt;
    bool everHeard;
    bool saysOnline;
    bool inAlarm;
};

Watched devices[] = {
    { "tank-01", 60000,  0, false, false, false },
    { "pump-2",  60000,  0, false, false, false },
    { "bore-7",  300000, 0, false, false, false },   // cellular, slower
};
const size_t DEVICE_COUNT = sizeof devices / sizeof devices[0];

// Pulls "tank-01" out of "plant/tank-01/level" - the SECOND level, which is
// where this site's topic layout puts the device name. Everything here uses one
// namespace per device, which is what makes this possible at all, and is why a
// topic layout is worth deciding before the first device is installed.
//
// The second level is matched EXACTLY rather than searched for. A substring
// search would be shorter and would match "tank-01" inside "tank-012", which
// is the sort of thing that works for two years and then does not.
Watched* deviceFor(const char* topic) {
    const char* name = strchr(topic, '/');
    if (name == nullptr) return nullptr;
    name++;

    const char* end = strchr(name, '/');
    const size_t length = end ? (size_t)(end - name) : strlen(name);

    for (size_t i = 0; i < DEVICE_COUNT; i++) {
        if (strlen(devices[i].name) == length &&
            strncmp(name, devices[i].name, length) == 0) {
            return &devices[i];
        }
    }
    return nullptr;
}

void onAnyReading(const char* topic, const MqttPayload& payload) {
    Watched* device = deviceFor(topic);
    if (device == nullptr) return;

    // A RETAINED message is the broker repeating the last thing said, not
    // somebody saying it now. Counting it as a sign of life would mark every
    // device healthy the moment this panel connected, dead ones included.
    if (payload.retained()) return;

    device->lastHeardAt = millis();
    device->everHeard = true;
}

void onStatus(const char* topic, const MqttPayload& payload) {
    Watched* device = deviceFor(topic);
    if (device == nullptr) return;

    // This one IS read when retained: the status topic is meant to be the last
    // known state, and that is exactly the question being asked.
    device->saysOnline = payload.toBool(false);
}

void raise(Watched& device, const char* why) {
    if (device.inAlarm) return;

    char topic[64];
    snprintf(topic, sizeof topic, "plant/panel-1/alarm/%s", device.name);

    // The flag is set only if the alarm was ACCEPTED. Setting it first and
    // publishing second would mean an alarm refused by a full queue is never
    // retried - a panel silently sitting on a fault it thinks it reported.
    // Left false, the next pass tries again.
    if (!mqtt.publish(topic, why, MQ_QOS_1, true)) return;
    device.inAlarm = true;

    Serial.print("ALARM ");
    Serial.print(device.name);
    Serial.print(": ");
    Serial.println(why);
}

void clear(Watched& device) {
    if (!device.inAlarm) return;

    char topic[64];
    snprintf(topic, sizeof topic, "plant/panel-1/alarm/%s", device.name);

    // An EMPTY retained payload is how MQTT deletes a retained message
    // [V5:3.3.1.3]. Publishing "ok" instead would leave the alarm in the
    // broker's store for ever, and the next screen to connect would show it.
    //
    // And the same rule as raise(): only believe it is cleared once the
    // clearing message has been accepted.
    if (!mqtt.publish(topic, "", MQ_QOS_1, true)) return;
    device.inAlarm = false;
}

void setup() {
    Serial.begin(115200);
    pinMode(PIN_ALARM_LAMP, OUTPUT);

    Ethernet.begin(MAC, STATIC_IP);

    mqtt.statusTopic("plant/panel-1/status");

    // '+' matches exactly one level, so this is every device's readings and
    // nothing else. '#' would also pull in the alarms this panel publishes
    // itself, which is a loop worth not building.
    mqtt.onMessage("plant/+/level", &onAnyReading, MQ_QOS_0);
    mqtt.onMessage("plant/+/state", &onAnyReading, MQ_QOS_0);
    mqtt.onMessage("sites/+/level", &onAnyReading, MQ_QOS_0);

    mqtt.onMessage("plant/+/status", &onStatus, MQ_QOS_1);
    mqtt.onMessage("sites/+/status", &onStatus, MQ_QOS_1);

    mqtt.connect(BROKER, "panel-1");
}

void loop() {
    mqtt.poll();
    Ethernet.maintain();

    // A panel that cannot reach the broker knows NOTHING about any device, and
    // must not claim they are all fine. It also must not claim they are all in
    // alarm - that would be a broken panel raising forty false alarms.
    if (!mqtt.isConnected()) {
        digitalWrite(PIN_ALARM_LAMP, (millis() / 500) % 2);   // flash: no data
        return;
    }

    bool anyAlarm = false;

    for (size_t i = 0; i < DEVICE_COUNT; i++) {
        Watched& device = devices[i];

        // Nothing heard since this panel started is NOT an alarm yet - it is
        // "not known", and the two go to different people: one to a technician
        // with a ladder, the other to whoever commissioned the system.
        if (!device.everHeard) continue;

        const unsigned long silentFor = millis() - device.lastHeardAt;

        // Twice the promised interval before alarming. One missed message is a
        // dropped packet; two is a device.
        if (silentFor > device.expectedEveryMs * 2) {
            raise(device, "no data");
        } else if (!device.saysOnline) {
            raise(device, "reported offline");
        } else {
            clear(device);
        }

        if (device.inAlarm) anyAlarm = true;
    }

    digitalWrite(PIN_ALARM_LAMP, anyAlarm ? HIGH : LOW);
}
