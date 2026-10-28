# A Sparkplug host through the Application API

A Sparkplug B 3.0 **Host Application** on a broker it does not own, with no
MQTT library: `modbee-mqtt serve` holds the MQTT connection for it
([Connections](../../../docs/app-api.md#connections-to-other-brokers)), hands
over what arrives on it, and publishes for it - a QoS 1 publish answered only
once the remote broker has acknowledged it. And the Eclipse Sparkplug TCK's
Host Application profile, run against it the same way.

**The server has a host of its own now.** `Sparkplug.PutHost` (`modbee-mqtt
sparkplug-hosts put`) runs the library's TCK-tested host on one of the
server's connections, and `ConsumeOpen.sparkplug` delivers Sparkplug payloads
decoded ([app-api.md](../../../docs/app-api.md#sparkplug-b)), so most
applications need neither this host nor a codec of their own. This example is
the other way: a host built on nothing but `Connections` and `Messages`.

| File | |
| --- | --- |
| `src/api.ts` | The Application API calls it uses: `Connections.Put/Delete/Reconnect/Watch`, `Messages.Consume/Publish` |
| `src/host.ts` | The host: STATE Will and birth (the same timestamp, a fresh one each connect), the goodbye before an intentional disconnect, seq with a reorder timeout and one rebirth per gap, DDEATH/NDEATH marking devices, nodes and metrics offline and stale, NCMD/DCMD |
| `src/tck.ts` | Runs the TCK's host tests against it, driving the kit over its control topics - also through the API |
| `tck/` | The kit's broker: HiveMQ CE with the TCK v3.0.0 extension |

The Sparkplug codec, topic grammar and STATE timestamp rule are the
platform's, imported read-only from `modbee-iot-platform/packages/sparkplug`,
which must be checked out beside this repository (and built: `npm run build`
there).

## Running the TCK

```sh
docker build -t modbee-sparkplug-tck tck/
docker run -d --name modbee-tck --memory 1g -p 127.0.0.1:11883:1883 modbee-sparkplug-tck

modbee-mqtt serve --config modbee.yaml        # with api: grpc: [unix:/tmp/modbee-api.sock]

npm install
MODBEE_API=unix:/tmp/modbee-api.sock npm run tck
```

```
─── Eclipse Sparkplug TCK v3.0.0 · Host Application profile, through the Application API ───
    kit broker 127.0.0.1:11883, host id "modbee-tck-host", API unix:/tmp/modbee-api.sock

  PASS          SessionEstablishmentTest  (46 passed, 0 failed, 1 not executed)
                OVERALL: PASS but INCOMPLETE
                message-flow-phid-sparkplug-clean-session-311: NOT EXECUTED
  PASS          SessionTerminationTest  (9 passed, 0 failed, 1 not executed)
                OVERALL: PASS but INCOMPLETE
                host-topic-phid-death-payload-timestamp-disconnect-with-no-disconnect-packet: NOT EXECUTED
  PASS          EdgeSessionTerminationTest  (15 passed, 0 failed)
                OVERALL: PASS
  PASS          MessageOrderingTest  (22 passed, 0 failed)
                OVERALL: PASS
  PASS          SendCommandTest  (39 passed, 0 failed)
                OVERALL: PASS
  SKIP          MultipleBrokerTest
                needs a second broker for the host to fail over to (item 6: client broker failover)

  5 of 5 run passed, 1 skipped; 131 assertions passed, 0 failed.
```

The two not executed cannot be by this host: the first is the MQTT 3.1.1
clean-session rule (it connects with 5.0), the second the path for a host
that vanishes without a DISCONNECT (it always sends one, after its goodbye).
`MultipleBrokerTest` waits for client broker failover.

**What the kit asks a person.** `EdgeSessionTerminationTest` asks six times
whether the host marked a device, a node and their metrics offline and stale;
the runner answers each from the host's own model, `PASS` only when the model
shows it. `SendCommandTest` asks for four commands; the runner has the host
send them.

A pass means "passes the TCK Host Application profile". It does **not** mean
"Sparkplug Compatible" - a trademark with an Eclipse Foundation listing
process.
