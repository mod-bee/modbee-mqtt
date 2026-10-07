// A PUMP THAT MUST BE TOLD TO STOP - AND MUST CONFIRM IT STOPPED
//
// The job: a supervisory system sends a command. The pump acts on it, and the
// supervisor has to know it happened - not that the message was sent, which is
// a different fact and a far weaker one.
//
// THREE THINGS MAKE THIS SAFE, AND ALL THREE ARE NEEDED:
//
//   1. THE COMMAND ARRIVES AT LEAST ONCE. QoS 1 on the subscription, so a
//      command is not lost to a dropped packet. It may arrive TWICE - that is
//      what "at least once" means - so acting on it must be idempotent.
//      "stop" twice is still stopped; "toggle" twice is a disaster, which is
//      why the commands here are states and not actions.
//
//   2. THE STATE IS PUBLISHED BACK, RETAINED. Not "I received your command" -
//      what the pump is ACTUALLY doing, read back from the contactor's
//      auxiliary contact. A command echoed back is a device agreeing with
//      itself.
//
//   3. THE CONFIRMATION IS ACKNOWLEDGED. publishAndConfirm() tells this sketch
//      when the broker has taken responsibility for the state message, so a
//      pump that stopped into a dead link knows the supervisor has not been
//      told and can retry when the link returns.

#include <WiFi.h>
#include <ModBeeMqtt.h>

const char* WIFI_SSID     = "plant-wifi";
const char* WIFI_PASSWORD = "";
const char* BROKER        = "192.168.1.10";

const uint8_t PIN_CONTACTOR = 5;      // output to the starter
const uint8_t PIN_RUNNING   = 6;      // auxiliary contact back from it

WiFiClient net;
MqttDevice mqtt(net);

const char* TOPIC_COMMAND = "plant/pump-2/command";
const char* TOPIC_STATE   = "plant/pump-2/state";
const char* TOPIC_STATUS  = "plant/pump-2/status";

bool commanded = false;          // what we were last told to do
bool running = false;            // what the contactor says is happening
bool statePublished = false;     // and whether the supervisor has been told

void onStateConfirmed(void*, MqttResult result, uint16_t) {
  // Exactly one of these arrives for every publishAndConfirm(), whatever
  // happens: acknowledged, refused, or lost with the connection.
  if (mq_succeeded(result)) {
    statePublished = true;
    return;
  }

  // Not delivered. Leaving statePublished false means the loop below sends it
  // again once there is a link - which is OUR retry, at the application level,
  // where it belongs. The protocol forbids resending a message mid-session and
  // this library does not do it.
  statePublished = false;
  Serial.print("state not confirmed: ");
  Serial.println(mq_resultName(result));
}

void onCommand(const char* topic, const MqttPayload& payload) {
  (void)topic;

  // A command that is not one of the words we understand is IGNORED, loudly.
  // Guessing what "stp" meant is how a pump ends up running at 3 a.m.
  const bool wantRunning = payload.toBool(commanded);
  if (!payload.equals("start") && !payload.equals("stop") &&
      !payload.equals("1") && !payload.equals("0")) {
    Serial.print("command not understood: ");
    Serial.println(payload.text());
    return;
  }

  commanded = wantRunning;
  digitalWrite(PIN_CONTACTOR, commanded ? HIGH : LOW);
}

void setup() {
  Serial.begin(115200);
  pinMode(PIN_CONTACTOR, OUTPUT);
  pinMode(PIN_RUNNING, INPUT_PULLUP);
  digitalWrite(PIN_CONTACTOR, LOW);

  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) delay(250);

  mqtt.statusTopic(TOPIC_STATUS);

  // The session is remembered, so a command sent while this pump was briefly
  // off the network is delivered when it returns rather than lost.
  mqtt.rememberSession(3600);

  // QoS 1: the command arrives at least once. See note 1 above about what
  // that costs and why every command here is a state rather than an action.
  mqtt.onMessage(TOPIC_COMMAND, &onCommand, MQ_QOS_1);

  mqtt.connect(BROKER, "pump-2");
}

void loop() {
  mqtt.poll();

  // WHAT THE PUMP IS ACTUALLY DOING, not what it was told.
  const bool nowRunning = digitalRead(PIN_RUNNING) == LOW;
  if (nowRunning != running) {
    running = nowRunning;
    statePublished = false;
  }

  // Say so until the broker has confirmed it. Retained, so a supervisor that
  // restarts reads the true state immediately instead of waiting for the pump
  // to change.
  if (!statePublished && mqtt.isConnected()) {
    // ONLY MARK IT SENT IF IT WAS ACCEPTED. publishAndConfirm() returns false
    // when the message never entered the queue at all - the queue is full, or
    // the broker will not take it at this QoS - and in that case the handler
    // NEVER RUNS. Setting the flag first and trusting the handler to clear it
    // would leave this pump believing it had told the supervisor, for ever,
    // which is the exact failure this sketch exists to prevent.
    statePublished = mqtt.publishAndConfirm(
        TOPIC_STATE, running ? "running" : "stopped",
        MQ_QOS_1, &onStateConfirmed, nullptr, /*retain=*/true);

    if (!statePublished) {
      Serial.print("state not even queued: ");
      Serial.println(mqtt.lastErrorText());
    }
  }

  // A command that never took effect is worth shouting about: the contactor
  // may have failed, or its auxiliary contact may have.
  static unsigned long disagreedSince = 0;
  if (commanded != running) {
    if (disagreedSince == 0) disagreedSince = millis();
    if (millis() - disagreedSince > 5000) {
      disagreedSince = millis();
      mqtt.publish("plant/pump-2/alarm", "commanded state not reached",
                   MQ_QOS_1, true);
    }
  } else {
    disagreedSince = 0;
  }
}
