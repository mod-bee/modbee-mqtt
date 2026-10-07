# Recipes — five jobs, done properly

Not features. Jobs, of the kind that turn up on a real site, with the parts
that are easy to get half-right called out.

| | |
| --- | --- |
| [A tank level on a dashboard](#1-a-tank-level-on-a-dashboard) | retain, deadband, heartbeat |
| [A pump that must confirm it stopped](#2-a-pump-that-must-confirm-it-stopped) | QoS 1, idempotence, read-back |
| [A site that keeps running when the link drops](#3-a-site-that-keeps-running-when-the-link-drops) | a local broker and a bridge |
| [A device whose silence must raise an alarm](#4-a-device-whose-silence-must-raise-an-alarm) | the Will, and why it is not enough |
| [A value you must not act on if it is stale](#5-a-value-you-must-not-act-on-if-it-is-stale) | freshness, and the retained trap |

---

## 1. A tank level on a dashboard

**The job.** A 4-20 mA level transmitter. The control room screen shows it, the
historian logs it, and a screen opened at three in the morning shows the level
rather than a blank box.

**The three decisions, and they are separate:**

### Retain it

A dashboard that connects after the last publish has missed it. On a tank
sampled every half hour that is thirty minutes of nothing.

```cpp
mqtt.publish("plant/tank-01/level", level, MQ_QOS_0, /*retain=*/true);
```

The broker keeps the last one and hands it to whoever subscribes next.

### A deadband, so a noisy input does not flood the link

```cpp
const float DEADBAND_METRES = 0.02f;

if (fabsf(level - lastPublished) >= DEADBAND_METRES) {
    mqtt.publish("plant/tank-01/level", level, MQ_QOS_0, true);
    lastPublished = level;
}
```

Without one, a floating-point reading from a real sensor publishes **every
scan**, because the last digit never sits still.

### A heartbeat, so a steady tank is not mistaken for a dead one

This is the part everybody leaves out, and it is the part that matters.

With a deadband alone, a tank that has not moved for an hour and a transmitter
that died an hour ago look **identical** to every subscriber. Nothing
downstream can tell them apart, because there is nothing to tell them apart
with.

```cpp
const unsigned long HEARTBEAT_MS = 30000;

const bool moved = fabsf(level - lastPublished) >= DEADBAND_METRES;
const bool quietTooLong = (millis() - lastPublishedAt) >= HEARTBEAT_MS;

if (moved || quietTooLong) {
    mqtt.publish("plant/tank-01/level", level, MQ_QOS_0, true);
    lastPublished = level;
    lastPublishedAt = millis();
}
```

Now silence means something, and recipe 4 can act on it.

**QoS 0 is right here.** If one reading is lost, another is along in thirty
seconds. Paying for acknowledgements on telemetry buys nothing.

### All three, in one line

Having read why each part is there, this is how it is actually written:

```cpp
float tankLevel = 0.0f;
mqtt.report("plant/tank-01/level", &tankLevel, /*deadband*/0.02, /*heartbeat*/30000);
```

Retained, deadbanded and heartbeated, checked on every `poll()`, and republished
in full whenever the connection comes back — so the loop only has to keep
`tankLevel` true.

> `examples/arduino/Tank_Level_Ethernet` — over wired Ethernet, which is what a
> panel usually is.
>
> For binary encodings and a quality that goes stale on schedule:
> [typed-values.md](typed-values.md).

---

## 2. A pump that must confirm it stopped

**The job.** A supervisory system sends "stop". The pump stops. The supervisor
must *know* it stopped — not that the message was sent, which is a different
and much weaker fact.

### Send states, never actions

QoS 1 means **at least once**. The same command can arrive twice — that is not
a defect, it is what the level guarantees.

So every command must be safe to apply twice:

```
plant/pump-2/command   ->  "start" | "stop"        safe to repeat
plant/pump-2/command   ->  "toggle"                a pump you did not want running
```

`"stop"` applied twice is still stopped. `"toggle"` applied twice is an
incident. **This is the single most important rule in the recipe.**

### Subscribe at QoS 1, and remember the session

```cpp
mqtt.rememberSession(3600);
mqtt.onMessage("plant/pump-2/command", &onCommand, MQ_QOS_1);
```

A command sent while the pump was briefly off the network is then delivered
when it returns, instead of being lost.

### Refuse what you do not understand

```cpp
void onCommand(const char*, const MqttPayload& payload) {
    if (!payload.equals("start") && !payload.equals("stop")) {
        Serial.print("command not understood: ");
        Serial.println(payload.text());
        return;                              // and do NOT guess
    }
    digitalWrite(PIN_CONTACTOR, payload.toBool() ? HIGH : LOW);
}
```

Working out what `"stp"` probably meant is how a pump runs at three in the
morning.

### Publish what is TRUE, not what you were told

```cpp
const bool running = digitalRead(PIN_RUNNING) == LOW;   // the aux contact
```

Echoing the command back is a device agreeing with itself. The state topic must
come from the contactor, so a command that did not take effect is visible.

### Confirm the confirmation

```cpp
void onStateConfirmed(void*, MqttResult result, uint16_t) {
    if (mq_succeeded(result)) statePublished = true;
    else statePublished = false;      // sent again when there is a link
}

mqtt.publishAndConfirm("plant/pump-2/state", running ? "running" : "stopped",
                       MQ_QOS_1, &onStateConfirmed, nullptr, /*retain=*/true);
```

Exactly one conclusion arrives for every accepted message. A pump that stopped
into a dead link then *knows* the supervisor has not been told, and says so
again when it can.

**Note that the retry is yours, at the application level.** The protocol
forbids resending a message mid-session — `[V5:MQTT-4.4.0-1]` — and this
library has no retry timer, deliberately. What it gives you instead is a
truthful answer about what happened, which is what you need to decide.

### Or ask the question properly

Everything above is the version that works on **both** protocol versions. At
5.0 there is a shorter way to say the same thing, and it removes the part
everyone gets wrong — the supervisor having to know which reply belongs to
which command:

```cpp
// The supervisor
mqtt.onMessage("plant/gateway/replies", &onReply, MQ_QOS_1);
mqtt.publishRequest("plant/pump-2/command", "stop",
                    "plant/gateway/replies", "stop-42");

// The pump - names neither the reply topic nor the id
void onCommand(const char*, const MqttPayload& request) {
    digitalWrite(PIN_CONTACTOR, LOW);
    mqtt.reply(request, digitalRead(PIN_RUNNING) == LOW ? "running" : "stopped");
}
```

The id comes back with the answer, so a supervisor with five commands
outstanding knows which one was answered. Nothing else changes: the state topic
above is still what a dashboard reads, because a reply goes to one asker and a
retained state goes to everybody.

**Do not use this instead of the read-back.** A reply says "I acted"; the state
topic says "this is what the contactor is doing". They are different facts and
the second is the one worth alarming on.

### And alarm on disagreement

```cpp
if (commanded != running && millis() - disagreedSince > 5000) {
    mqtt.publish("plant/pump-2/alarm", "commanded state not reached",
                 MQ_QOS_1, true);
}
```

The contactor can fail. So can its auxiliary contact. Neither is visible from
the command side alone.

> `examples/arduino/Pump_Confirm_Stop`

---

## 3. A site that keeps running when the link drops

**The job.** A remote site — a pump station, a reservoir, an outstation — with
a link to head office that is not reliable. When the link goes, the site must
carry on. When it comes back, head office must catch up.

### The wrong shape

Every device on site connecting directly to a broker at head office. Now the
tank cannot tell the pump *standing next to it* anything without a working WAN
connection.

### The right shape

**A broker on the site.** Everything local talks to it — local traffic never
leaves the building, and local control works whether or not anyone is
reachable. A **bridge** forwards the subset head office needs.

```
  tank-01 ─┐
  pump-2  ─┼─► site broker ══bridge══► head office broker ─► dashboards, historian
  panel-1 ─┘      (local control keeps working regardless)
```

This library can be that site broker, in your own binary — see
[broker.md](broker.md) — and the bridge — see [bridge.md](bridge.md).

### What the devices do differently

Nothing at all. They point at the site broker instead of head office. That is
the whole change.

### What the bridge must not be

**A bridge that forwards both ways on overlapping topics is a loop**, and one
message will saturate both sites. This library **refuses to start** such a
configuration rather than trying to be clever:

```cpp
MqttBridgeTopic up;
up.filter = "data/#";
up.direction = MqttBridgeDirection::Out;
up.remotePrefix = "site7/";                  // publishes into site7/data/...

MqttBridgeTopic down;
down.filter = "cmd/#";
down.direction = MqttBridgeDirection::In;    // subscribes to cmd/... only
```

Disjoint topic spaces per direction, and a rule whose two directions could
carry each other's output is **refused at start-up** rather than accepted and
regretted. A prefix is *not* enough on its own — `plant/+/c` and `plant/b/#`
share no literal text but both match `plant/b/c`, so the check is a real
overlap test rather than a string comparison.

### What survives the outage

On each device:

```cpp
mqtt.rememberSession(3600);   // subscriptions and in-flight messages survive
mqtt.queueSize(64);           // and there is room to hold new ones
```

`mqtt.queued()` shows the queue filling during the outage and draining
afterwards. If it stops falling, it is full and publishes are being **refused
and reported** — never silently dropped.

---

## 4. A device whose silence must raise an alarm

**The job.** Somebody must find out when a device stops, without waiting for a
person to notice a stale number.

### The Will — half the answer

A device that loses power cannot say goodbye. So the broker says it on its
behalf:

```cpp
mqtt.statusTopic("plant/tank-01/status");
```

One line, and all three parts are done:

1. `"offline"` registered as the Will, retained
2. `"online"` published, retained, the moment the session comes up
3. `"offline"` published *by the device itself* on a deliberate `disconnect()`,
   because a Will is **not** sent on a graceful goodbye — a device that shut
   down cleanly would otherwise read as online for ever

**Both halves are needed.** A Will with nothing ever publishing "online" is an
alarm that can never clear. This is the most-forgotten half of most MQTT
deployments and it is why the call does both.

### Why the Will is not enough

The Will fires when the **TCP connection** drops. It does not fire when:

- the device is wedged but its network stack is still answering
- the program crashed and the watchdog is holding the network card up
- the sensor failed but the board is perfectly healthy

All three keep the connection open and publish nothing. To an alarm system
watching the Will alone, that device is fine.

### So watch for silence as well

```cpp
if (!device.everHeard) continue;                 // "not known" is NOT an alarm

const unsigned long silentFor = millis() - device.lastHeardAt;
if (silentFor > device.expectedEveryMs * 2) raise(device, "no data");
else if (!device.saysOnline)                raise(device, "reported offline");
else                                         clear(device);
```

Three things there earn their place:

- **Twice the promised interval.** One missed message is a dropped packet; two
  is a device. And the interval is per device — a tank on thirty seconds and a
  cellular outstation on five minutes cannot share a constant.
- **Never heard is not an alarm.** It is "not known", and it sends somebody
  different: a commissioning error, not a fault.
- **This needs recipe 1's heartbeat.** Without a device publishing when nothing
  has changed, silence means nothing at all.

### Do not count retained messages as signs of life

```cpp
if (payload.retained()) return;      // the broker repeating, not somebody speaking
```

A panel that skipped this would mark every device healthy the instant it
connected — including the ones that died last Tuesday, whose retained readings
arrive in the first second.

### Clearing an alarm

```cpp
mqtt.publish("plant/panel-1/alarm/tank-01", "", MQ_QOS_1, true);
```

An **empty** retained payload deletes the retained message `[V5:3.3.1.3]`.
Publishing `"ok"` instead leaves the alarm in the broker's store for ever, and
the next screen to connect will show it.

### And a panel that cannot reach the broker knows nothing

```cpp
if (!mqtt.isConnected()) {
    digitalWrite(PIN_ALARM_LAMP, (millis() / 500) % 2);   // flash: no data
    return;                                               // claim nothing
}
```

It must not report every device healthy, and it must not raise forty alarms.
"I cannot see" is its own state.

> `examples/arduino/Alarm_On_Silence`

---

## 5. A value you must not act on if it is stale

**The job.** A gateway turns a pump on when a tank is low. It must never do
that on a reading that is no longer true.

### The trap MQTT sets

A connected broker will hand you a retained reading from a device that died
last Tuesday, and say nothing about it. Your variable holds `4.2`. There is
nothing in it that says when.

Worse: the link is up, the broker is healthy, the subscription is working. Every
signal you would normally check says fine.

### Two facts, both required

```cpp
if (mqtt.isFresh("plant/tank-01/level", 30000)) {
    // a message arrived within 30 s AND the link is up
    if (level < 8.0) startThePump();
} else {
    // do not act. mqtt.sinceMessage(...) says how long it has been
}
```

`isFresh()` is false when the link is down **however recent the value is** —
because a link that has been down for ten minutes means the last value is at
least ten minutes old whatever its timestamp says.

### The interval is yours, and it has to be

There is no constant that is right for a temperature sampled hourly and a
pressure sampled ten times a second. `isFresh()` takes the number **you**
promised, per topic.

### Three states, not two

```
never received          ->  Unknown      a commissioning error: send an engineer
received, too old       ->  Bad          a fault: send a technician
received, within promise->  Good
```

`Unknown` and `Bad` must stay distinct. One means it was never wired up; the
other means it stopped. They go to different people.

### When you want the library to judge it

The typed layer does the full version — quality, staleness, and a fallback that
a control path cannot accidentally act on:

```cpp
poller.track("plant/tank-01/level", MqttValueType::Float64, /*every*/ 30000);

const double level = poller.number("plant/tank-01/level", /*fallback=*/-1.0);
// the fallback unless the quality is Good, so a value the library has already
// said it distrusts cannot reach a control decision by accident
```

See [typed-values.md](typed-values.md).

> `examples/cpp/gateway` — the whole thing, on a host.

---

## The thread running through all five

Every one of these is the same idea in a different costume:

**MQTT tells you what was said. It does not tell you whether it is still true,
and it will not tell you when something stops speaking.**

The protocol gives you the parts — the Will, retain, QoS, session state — and
this library gives you the honest answer at each step: what was delivered, what
was refused, what is queued, and how long it has been since anybody spoke. What
none of it does is decide what "current" means for your process. That number is
yours, and it is the one worth arguing about at commissioning.
