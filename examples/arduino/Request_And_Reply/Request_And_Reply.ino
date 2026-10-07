// ASK A DEVICE SOMETHING, AND GET AN ANSWER BACK
//
// The job: a small gateway asks a valve for its position, and the valve
// answers. Plain publish and subscribe cannot do this on their own - you would
// have to invent a reply topic, agree it at both ends, and invent a way to
// tell one answer from another when three questions are outstanding.
//
// MQTT 5.0 has all three built in, and this library reads them off the message
// so neither end has to:
//
//   Response Topic    [V5:3.3.2.3.5]  where the answer goes
//   Correlation Data  [V5:3.3.2.3.6]  which question it answers
//
// THE ANSWERING END NAMES NEITHER. It calls reply() and the library takes the
// topic and the correlation id from the request it was handed. That is what
// makes this work when there are forty devices and one gateway.
//
// 5.0 only. On a 3.1.1 connection both calls are REFUSED rather than sent
// without the parts that make them work - an answer nothing can be matched to
// is worse than no answer, and this sketch prints that rather than looking
// broken.

#include <Ethernet.h>
#include <ModBeeMqtt.h>

byte MAC[] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xFE, 0x21 };
const char* BROKER = "192.168.1.10";

EthernetClient net;
MqttDevice mqtt(net);

// This device plays BOTH parts, so one sketch shows both halves. On a real
// site they are two boards.
const char* TOPIC_ASK   = "plant/valve-3/query";
const char* TOPIC_REPLY = "plant/gateway-1/replies";

const uint8_t PIN_VALVE_OPEN = 4;      // limit switch

unsigned long outstanding = 0;         // which question we are waiting on
unsigned long answered = 0;

// --- The answering end ---
void onQuery(const char* topic, const MqttPayload& request) {
  (void)topic;

  // A message with no Response Topic is not a question. Answering it would
  // mean choosing a topic ourselves, and a reply published where nobody is
  // listening is worse than none: we would believe we had answered.
  if (!request.isRequest()) {
    Serial.println("a query with nowhere to answer - ignored");
    return;
  }

  const bool open = digitalRead(PIN_VALVE_OPEN) == LOW;

  // Neither the reply topic nor the correlation id appears here. Both come off
  // the request, which is the whole point.
  //
  // Checked, because an answer that was never queued leaves the asker waiting
  // and nothing else will notice on this side.
  if (!mqtt.reply(request, open ? "open" : "closed")) {
    Serial.print("could not answer: ");
    Serial.println(mqtt.lastErrorText());
  }
}

// --- The asking end ---
void onReply(const char* topic, const MqttPayload& payload) {
  (void)topic;

  // The correlation data is what the request was sent with, byte for byte.
  // With three questions outstanding this is how you know which one came back.
  const char* id = (const char*)payload.correlationData();
  Serial.print("answer to ");
  Serial.print(id ? id : "(no id)");
  Serial.print(": ");
  Serial.println(payload.text());
  answered++;
}

void setup() {
  Serial.begin(115200);
  pinMode(PIN_VALVE_OPEN, INPUT_PULLUP);

  Ethernet.begin(MAC);

  mqtt.statusTopic("plant/gateway-1/status");

  // QoS 1 on both: a question nobody received is a caller waiting for ever,
  // and an answer nobody received is the same problem twice.
  mqtt.onMessage(TOPIC_ASK, &onQuery, MQ_QOS_1);
  mqtt.onMessage(TOPIC_REPLY, &onReply, MQ_QOS_1);

  mqtt.connect(BROKER, "gateway-1");
}

void loop() {
  mqtt.poll();
  Ethernet.maintain();

  // Ask once every ten seconds.
  static unsigned long lastAsk = 0;
  if (mqtt.isConnected() && millis() - lastAsk >= 10000) {
    lastAsk = millis();

    // The id has to be unique among the questions still outstanding. A counter
    // is enough - it never has to mean anything to anybody else.
    char id[16];
    snprintf(id, sizeof id, "q-%lu", ++outstanding);

    if (!mqtt.publishRequest(TOPIC_ASK, "position?", TOPIC_REPLY, id)) {
      // The usual reason: a 3.1.1 broker, which has no properties at all and
      // therefore nowhere to put the reply topic or the id.
      Serial.print("request refused: ");
      Serial.println(mqtt.lastErrorText());
      Serial.print("connection is MQTT ");
      Serial.println(mq_versionName(mqtt.activeVersion()));
    }
  }

  // A question that never came back is worth noticing. The protocol will not
  // tell you - it has no idea you asked one.
  if (outstanding > answered + 3) {
    static unsigned long lastComplaint = 0;
    if (millis() - lastComplaint >= 30000) {
      lastComplaint = millis();
      mqtt.publish("plant/gateway-1/alarm", "valve-3 is not answering",
                   MQ_QOS_1, true);
    }
  }
}
