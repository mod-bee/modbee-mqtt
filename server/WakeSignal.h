// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// A descriptor a shard's readiness set waits on, made readable from any
// thread: how a thread with work for a shard wakes it.
//
//   Linux     an eventfd
//   Windows   a UDP socket on loopback, sent to itself - the readiness set
//             (wepoll) watches sockets only
//
// signal() from any thread; drain() on the thread that waits, before it
// looks for the work, so a signal that arrives after the drain wakes it again.

class WakeSignal {
public:
    WakeSignal() = default;
    ~WakeSignal() { close(); }
    WakeSignal(const WakeSignal&) = delete;
    WakeSignal& operator=(const WakeSignal&) = delete;

    bool open();
    void close();
    bool isOpen() const { return _handle >= 0; }
    // For a readiness set (MqttPollSet::add); -1 when closed.
    int handle() const { return _handle; }
    void signal();
    void drain();

private:
    int _handle = -1;
};
