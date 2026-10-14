// A REMOTE SITE ON A CELLULAR LINK
//
// The job: a bore pump, a reservoir or a weather station, an hour's drive from
// anywhere, on an LTE-M or NB-IoT modem. The link is not reliable and nobody
// is going to visit when it stops.
//
// WHAT IS DIFFERENT ABOUT CELLULAR, AND IT IS NOT THE PROTOCOL:
//
//   The library is identical. TinyGsmClient is an Arduino network channel like
//   WiFiClient or EthernetClient, so everything below is the same code that
//   runs on a wired panel.
//
//   WHAT DIFFERS IS TIME. A modem's connect() is a sequence of AT commands and
//   can take SECONDS. Arduino's Client::connect() is synchronous on every core
//   and this library cannot make somebody else's network stack asynchronous -
//   so a reconnect attempt inside poll() would stall the loop for that long.
//
//   So this sketch turns auto-reconnect OFF and reconnects where it can afford
//   to. Nothing else in the library ever stalls: publishing, receiving and the
//   keepalive are all non-blocking whatever the bearer. docs/transports.md
//   covers this in full.

#define TINY_GSM_MODEM_SIM7600          // whichever modem you actually have
#include <TinyGsmClient.h>
#include <ModBeeMqtt.h>

const char* APN    = "iot.provider.net";
const char* BROKER = "mqtt.example.com";

TinyGsm modem(Serial1);
TinyGsmClient net(modem);
MqttDevice mqtt(net);

const char* TOPIC_LEVEL  = "sites/bore-7/level";
const char* TOPIC_STATUS = "sites/bore-7/status";
const char* TOPIC_SIGNAL = "sites/bore-7/signal";

bool bearerUp = false;
unsigned long nextConnectAttempt = 0;
unsigned long reconnectDelay = 5000;

// What the site is actually reporting. The library publishes it; the loop only
// has to keep it true.
float reservoirLevel = 0.0f;

bool bringBearerUp() {
  // All of this blocks, and all of it belongs here rather than in the loop.
  if (!modem.restart()) return false;
  if (!modem.waitForNetwork(60000)) return false;
  return modem.gprsConnect(APN);
}

void setup() {
  Serial.begin(115200);
  Serial1.begin(115200);

  bearerUp = bringBearerUp();

  mqtt.statusTopic(TOPIC_STATUS);

  // THE SETTING THAT MATTERS MOST ON A BAD LINK.
  //
  // The broker keeps our session for an hour, so a QoS 1 reading that was in
  // flight when the bearer dropped is retransmitted when it comes back, and
  // our subscriptions are still there. Without it every outage starts again
  // from nothing.
  mqtt.rememberSession(3600);

  // Room to hold readings through an outage. A publish past this is REFUSED
  // and says so - never dropped silently, which on a site nobody visits is the
  // difference between missing data and not knowing you are missing data.
  //
  // Any size: the queue is taken when the device connects, as many as asked,
  // and only memory refuses - then it keeps the size it had and
  // queueOutOfMemory() says so (checked after connect(), below). Being
  // quietly handed half of what you asked for is how a site loses an
  // afternoon of data and nobody can say why.
  mqtt.queueSize(64);

  // A longer keepalive, because every ping costs data and a cellular plan is
  // usually sold by the megabyte. The cost is that a dead link takes longer to
  // notice - that is the trade, and it should be a decision rather than a
  // default.
  mqtt.keepAlive(300);

  // Report the level when it moves 50 mm, and at least once every five
  // minutes so head office can tell a still reservoir from a dead outstation.
  // Five minutes rather than thirty seconds because every message costs data
  // on a plan sold by the megabyte - the heartbeat interval is a commercial
  // decision as much as a technical one.
  //
  // QoS 1, so a reading survives a bearer that drops mid-message. Nothing is
  // queued while the link is down: what head office needs when the site comes
  // back is the level NOW, not every value it passed through in the dark.
  mqtt.report(TOPIC_LEVEL, &reservoirLevel, 0.05, 300000, MQ_QOS_1);

  // OFF. See the note at the top: a reconnect inside poll() would stall the
  // loop for as long as the modem takes.
  mqtt.autoReconnect(false);

  if (bearerUp) mqtt.connect(BROKER, "bore-7");
  if (mqtt.queueOutOfMemory()) {
    Serial.print("no memory for a queue of 64 - it holds ");
    Serial.println((unsigned long)mqtt.queueCapacity());
  }
}

void loop() {
  mqtt.poll();          // never stalls, whatever the modem is doing

  // --- Reconnecting, on our terms ---
  if (!mqtt.isConnected() && millis() >= nextConnectAttempt) {
    // Back off, so a site with no coverage does not spend its battery and its
    // data allowance discovering that once a second.
    nextConnectAttempt = millis() + reconnectDelay;
    if (reconnectDelay < 300000) reconnectDelay *= 2;

    if (!modem.isGprsConnected()) {
      bearerUp = bringBearerUp();     // blocks, deliberately, and only here
    }
    if (bearerUp) mqtt.connect(BROKER, "bore-7");
  }
  if (mqtt.isConnected()) reconnectDelay = 5000;

  // --- The actual job ---
  //
  // Read the sensor. Publishing it is the library's, not this loop's.
  reservoirLevel = analogRead(A0) * (10.0f / 4095.0f);

  // Signal strength alongside the data. When somebody eventually asks why the
  // site went quiet on Tuesday, this is the answer - and it is published by
  // hand rather than reported, because it is a diagnostic rather than a
  // process value and hourly is plenty.
  static unsigned long lastSignal = 0;
  if (millis() - lastSignal >= 3600000) {
    lastSignal = millis();
    mqtt.publish(TOPIC_SIGNAL, modem.getSignalQuality(), MQ_QOS_0, true);
  }
}
