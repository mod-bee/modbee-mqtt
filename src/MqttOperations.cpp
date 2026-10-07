// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttOperations.h"

#include <string.h>

MqttOperations::MqttOperations() {
    // Reserved once so a conclusion reached inside a handler never allocates
    // on the poll path. The bound below is what keeps it from growing past it.
    _deferred.reserve(kMaxDrainPerDispatch);
}

void MqttOperations::setCapacity(size_t capacity) {
    if (capacity == 0) capacity = 1;
    if (capacity > MQTT_QUEUE_CAPACITY) capacity = MQTT_QUEUE_CAPACITY;
    _capacity = capacity;
}

int MqttOperations::indexOfOpId(uint32_t opId) const {
    if (opId == 0) return -1;
    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if (_ops[i].opId == opId) return i;
    }
    return -1;
}

int MqttOperations::indexOfPacketId(uint16_t packetId) const {
    if (packetId == MQ_PACKET_ID_NONE) return -1;
    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if (_ops[i].inUse() && _ops[i].packetId == packetId) return i;
    }
    return -1;
}

size_t MqttOperations::pending() const {
    size_t count = 0;
    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if (_ops[i].inUse()) count++;
    }
    return count;
}

size_t MqttOperations::sentCount() const {
    size_t count = 0;
    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if (_ops[i].inUse() && _ops[i].sent) count++;
    }
    return count;
}

MqttOperation* MqttOperations::findByPacketId(uint16_t packetId) {
    const int index = indexOfPacketId(packetId);
    return index >= 0 ? &_ops[index] : nullptr;
}

// =============================================================================
// QUEUEING
// =============================================================================

MqttResult MqttOperations::add(const MqttOperation& operation) {
    // Checked first, before anything else is considered. A queue that filled
    // and then decided what to do about it would have already spent the work.
    if (pending() >= _capacity) {
        _refused++;
        return MqttResult::QueueFull;
    }

    // --- Supersede ---
    //
    // Only a publish, only to the same topic, only while both are willing, and
    // only before either has been sent.
    if (operation.kind == MqttOpKind::Publish && operation.supersedable) {
        for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
            MqttOperation& existing = _ops[i];
            if (!existing.inUse()) continue;
            if (existing.kind != MqttOpKind::Publish) continue;
            if (!existing.supersedable) continue;
            if (existing.topic != operation.topic) continue;

            // ALREADY ON THE WIRE. Rewriting it now would leave the
            // acknowledgement, when it arrives, confirming a value that was
            // never transmitted. The new publish queues behind it instead.
            if (existing.sent) continue;

            // The same value twice is a duplicate, not a supersede. Telling a
            // caller its publish was superseded by an identical one would be
            // true and useless.
            if (existing.payload == operation.payload &&
                existing.qos == operation.qos &&
                existing.retain == operation.retain &&
                existing.properties == operation.properties) {
                _refused++;
                return MqttResult::Duplicate;
            }

            // Taken by value BEFORE the record is touched. The handler is
            // application code and may queue an operation of its own, which
            // would otherwise run while this record is half-rewritten.
            MqttCompletionFn handler = existing.onComplete;
            void* context = existing.context;
            const uint16_t oldPacketId = existing.packetId;

            // EVERY field the new operation carries, not just the payload. The
            // completion handler belongs to the caller that asked for it, so a
            // publish replacing another must not inherit the old one's terms.
            const uint32_t keptId = existing.opId;
            existing = operation;
            existing.opId = keptId;
            existing.sent = false;
            existing.packetId = MQ_PACKET_ID_NONE;

            _superseded++;

            // The record is fully written, so nothing here reads it again and
            // the handler is free to queue.
            dispatch(handler, context, MqttResult::Superseded, oldPacketId);
            return MqttResult::Queued;
        }
    }

    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if (_ops[i].inUse()) continue;
        _ops[i] = operation;
        _ops[i].priority = operation.priority || _priorityMode;
        _ops[i].opId = _nextOpId++;
        if (_nextOpId == 0) _nextOpId = 1;   // never zero: zero means unused
        _ops[i].sent = false;
        _ops[i].packetId = MQ_PACKET_ID_NONE;
        return MqttResult::Queued;
    }

    _refused++;
    return MqttResult::QueueFull;
}

// =============================================================================
// DRAINING
// =============================================================================

MqttOperation* MqttOperations::nextUnsent() {
    // In queue order. What was accepted first leaves first, because anything
    // carrying its own sequence numbers depends on it - and because a caller
    // that published A then B and saw B arrive first would have no way to
    // reason about either.
    //
    // Priority operations go first, and among themselves they are still in
    // queue order. There is exactly one source of them - what the
    // session-established hook queues - so this reorders the opening of a
    // connection and nothing else.
    uint32_t oldest = 0;
    int found = -1;
    bool foundPriority = false;

    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if (!_ops[i].inUse() || _ops[i].sent) continue;

        const bool isPriority = _ops[i].priority;
        if (found >= 0) {
            if (foundPriority && !isPriority) continue;          // priority wins
            if (foundPriority == isPriority && _ops[i].opId >= oldest) continue;
        }
        oldest = _ops[i].opId;
        found = i;
        foundPriority = isPriority;
    }
    return found >= 0 ? &_ops[found] : nullptr;
}

void MqttOperations::markSent(MqttOperation& operation, uint16_t packetId) {
    operation.sent = true;
    operation.packetId = packetId;
    // Stamped HERE, in the one place that marks an operation sent, so an
    // acknowledgement deadline cannot be started from the wrong moment.
    operation.sentAt = mq_millis();
}

void MqttOperations::markAllUnsent() {
    // The deadline is NOT cleared here. These are about to be offered again,
    // and markSent() restamps each one as it goes - so a session resumed after
    // an outage does not inherit a deadline that expired during it.
    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if (_ops[i].inUse()) _ops[i].sent = false;
    }
}

// =============================================================================
// CONCLUDING
// =============================================================================

void MqttOperations::retireAt(int index, MqttResult result) {
    if (index < 0 || index >= (int)MQTT_QUEUE_CAPACITY) return;
    if (!_ops[index].inUse()) return;

    // EVERYTHING NEEDED FROM THE RECORD IS TAKEN BEFORE IT IS ERASED, AND
    // NOTHING READS IT AFTERWARDS. This is the rule the whole class follows.
    MqttCompletionFn handler = _ops[index].onComplete;
    void* context = _ops[index].context;
    const uint16_t packetId = _ops[index].packetId;

    _ops[index] = MqttOperation();
    _concluded++;

    dispatch(handler, context, result, packetId);
}

size_t MqttOperations::withdraw(const std::vector<uint8_t>& topic,
                                MqttResult result) {
    // The live mask is taken first, exactly as concludeOverdue does: retiring
    // a record dispatches a completion handler, and a handler may queue
    // something new. Without the snapshot this could withdraw a publish the
    // handler had just made, which is not what the caller asked for.
    uint32_t live = liveMask();
    size_t taken = 0;
    for (int index = 0; index < (int)MQTT_QUEUE_CAPACITY; index++) {
        if ((live & (1u << index)) == 0) continue;
        if (!_ops[index].inUse()) continue;
        if (_ops[index].kind != MqttOpKind::Publish) continue;
        if (_ops[index].sent) continue;
        if (_ops[index].topic != topic) continue;
        retireAt(index, result);
        taken++;
    }
    return taken;
}

size_t MqttOperations::concludeOverdue(mq_time_t now, mq_time_t timeoutMs,
                                       MqttResult result) {
    if (timeoutMs == 0) return 0;   // no deadline; wait for ever, as before

    // THE SAME SNAPSHOT DISCIPLINE AS concludeAll(), and for the same reason:
    // retireAt() runs the handler synchronously and that handler may add(),
    // filling a slot this loop has not reached yet. Walking live state would
    // retire an operation that was never attempted.
    uint32_t live = liveMask();
    size_t concluded = 0;

    for (int index = 0; index < (int)MQTT_QUEUE_CAPACITY; index++) {
        if ((live & (1u << index)) == 0) continue;
        if (!_ops[index].inUse()) continue;
        if (!_ops[index].sent) continue;
        // QoS 0 has nothing to wait for: it concludes when it is written.
        if (_ops[index].packetId == MQ_PACKET_ID_NONE) continue;
        if (mq_elapsed(_ops[index].sentAt, now) < timeoutMs) continue;

        retireAt(index, result);
        concluded++;
    }
    return concluded;
}

void MqttOperations::concludeByPacketId(uint16_t packetId, MqttResult result) {
    retireAt(indexOfPacketId(packetId), result);
}

void MqttOperations::concludeByOpId(uint32_t opId, MqttResult result) {
    retireAt(indexOfOpId(opId), result);
}

// WHICH SLOTS ARE LIVE RIGHT NOW, as a bitmask.
//
// A COUNT was not enough, and the difference is a real defect. retireAt() runs
// the handler synchronously, and that handler may add() - which fills the
// first free slot. If that slot's index is HIGHER than the loop has reached,
// the loop arrives at it and retires an operation that was never attempted,
// with the failure result of the one before it. Worse, doing so consumed a
// unit of the count, so one of the ORIGINAL operations was skipped and left
// in use with its completion handler never called - exactly the "outstanding
// for ever" outcome the snapshot was there to prevent.
//
// A mask names the slots rather than counting them, so a slot filled during
// the walk is not one of them. MQTT_QUEUE_CAPACITY is 32 by default and the
// static_assert keeps that honest.
uint32_t MqttOperations::liveMask() const {
    static_assert(MQTT_QUEUE_CAPACITY <= 32,
                  "the live-slot snapshot is a uint32_t bitmask over the queue");
    uint32_t live = 0;
    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if (_ops[i].inUse()) live |= (1u << i);
    }
    return live;
}

void MqttOperations::concludeAll(MqttResult result) {
    const uint32_t live = liveMask();
    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if ((live & (1u << i)) == 0) continue;
        if (!_ops[i].inUse()) continue;   // a handler may have retired it already
        retireAt(i, result);
    }
}

void MqttOperations::concludeSent(MqttResult result) {
    const uint32_t live = liveMask();
    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if ((live & (1u << i)) == 0) continue;
        if (!_ops[i].inUse() || !_ops[i].sent) continue;
        retireAt(i, result);
    }
}

void MqttOperations::concludeUnsent(MqttResult result) {
    const uint32_t live = liveMask();
    for (int i = 0; i < (int)MQTT_QUEUE_CAPACITY; i++) {
        if ((live & (1u << i)) == 0) continue;
        if (!_ops[i].inUse() || _ops[i].sent) continue;
        retireAt(i, result);
    }
}

// =============================================================================
// DISPATCH
// =============================================================================

void MqttOperations::dispatch(MqttCompletionFn handler, void* context,
                              MqttResult result, uint16_t packetId) {
    if (!handler) return;

    if (_dispatchDepth > 0) {
        // A conclusion reached while a handler is already running is queued
        // rather than nested. Without this the depth is unbounded: a handler
        // told its publish was superseded that publishes again supersedes the
        // publish that replaced it, and the chain runs until the stack is gone.
        if (_deferred.size() < kMaxDrainPerDispatch * 4) {
            Deferred owed;
            owed.handler = handler;
            owed.context = context;
            owed.result = result;
            owed.packetId = packetId;
            _deferred.push_back(owed);
        }
        return;
    }

    // THE DEPTH IS RESTORED WHATEVER THE HANDLER DOES.
    //
    // handler() is the application's code. Under -fno-exceptions - what CI
    // builds and what a board gets - it cannot throw and this is simply a
    // counter. On a host build with exceptions it can, and then _dispatchDepth
    // was never decremented: dispatch() took the "already running" branch for
    // ever afterwards, drainDeferred() returned immediately, and every
    // completion callback in the client stopped firing permanently with
    // nothing anywhere reporting it. A guard costs nothing and removes the
    // question.
    struct DepthGuard {
        int& depth;
        explicit DepthGuard(int& d) : depth(d) { depth++; }
        ~DepthGuard() { depth--; }
    } guard(_dispatchDepth);

    handler(context, result, packetId);

    // Drained with an index rather than an iterator: a deferred handler may
    // defer another, which pushes onto this very vector. And bounded, because
    // "may defer another" has no natural end - every entry is a real
    // conclusion owed to somebody, so none is discarded, the drain just stops
    // and the rest are carried to the next call.
    size_t drained = 0;
    while (drained < _deferred.size() && drained < kMaxDrainPerDispatch) {
        const Deferred owed = _deferred[drained];
        drained++;
        if (owed.handler) owed.handler(owed.context, owed.result, owed.packetId);
    }

    if (drained < _deferred.size()) {
        _deferred.erase(_deferred.begin(), _deferred.begin() + (long)drained);
    } else {
        _deferred.clear();
    }
}

void MqttOperations::drainDeferred() {
    if (_dispatchDepth > 0 || _deferred.empty()) return;

    // Swapped out before any handler runs, for the same reason as everywhere
    // else here: a handler may queue work, which pushes onto this vector.
    std::vector<Deferred> owed;
    owed.swap(_deferred);

    // Guarded for the same reason as dispatch(): a handler that throws must
    // not leave this counter raised for the life of the process.
    struct DepthGuard {
        int& depth;
        explicit DepthGuard(int& d) : depth(d) { depth++; }
        ~DepthGuard() { depth--; }
    } guard(_dispatchDepth);

    for (size_t i = 0; i < owed.size(); i++) {
        if (owed[i].handler) {
            owed[i].handler(owed[i].context, owed[i].result, owed[i].packetId);
        }
    }
}
