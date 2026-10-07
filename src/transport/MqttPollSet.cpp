// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttPollSet.h"

#if defined(MQTT_PLATFORM_HOST)

#include "MqttSocketSupport.h"

#include <cstring>

// ONE SYSCALL PER WAIT, AND NO MEMBERSHIP TO KEEP IN STEP.
//
// poll(2) is handed the whole list each call and answers for all of it, so
// watching N descriptors costs one syscall and no bookkeeping. Windows spells
// it WSAPoll and it behaves the same way.
//
// WHY NOT epoll. It was tried first, and measured worse. epoll is O(1) in the
// kernel but the set has to be BUILT - one epoll_ctl per descriptor - and a
// set rebuilt each round to stay in step with a recycled connection pool cost
// 43,042 epoll_ctl calls to save 1,171 recvfrom calls at 200 clients. Keeping
// the set across rounds avoids that but means tracking membership against a
// pool whose slots are reused, and a stale registration is a descriptor that
// was closed and reopened as something else - the worst class of bug this
// library could carry.
//
// poll() scans its list in the kernel, so at a few thousand descriptors epoll
// with persistent membership would win again. That is a real refinement and
// it is not this: the syscall this removes is the one per CONNECTION, and
// poll() removes all of them for one.
#if defined(_WIN32)
    #include <winsock2.h>
    #define MQ_POLL WSAPoll
    typedef WSAPOLLFD MqPollFd;
#else
    #include <poll.h>
    #define MQ_POLL ::poll
    typedef struct pollfd MqPollFd;
#endif

#if defined(__linux__)
    #include <sys/epoll.h>
    #include <unistd.h>
    #define MQ_HAVE_EPOLL 1
#endif

namespace {
// Events collected per wait. Level-triggered, so anything beyond this is
// simply reported on the next wait rather than lost.
const size_t kEventsPerWait = 4096;
}

MqttPollSet::MqttPollSet() : _open(false) {}

MqttPollSet::~MqttPollSet() { end(); }

bool MqttPollSet::begin() {
    _open = true;
    return true;
}

void MqttPollSet::end() {
    _watched.clear();
    _ready.clear();
#if defined(MQ_HAVE_EPOLL)
    if (_epoll >= 0) ::close(_epoll);
#endif
    _epoll = -1;
    _open = false;
}

bool MqttPollSet::beginPersistent() {
#if defined(MQ_HAVE_EPOLL)
    end();
    _epoll = epoll_create1(EPOLL_CLOEXEC);
    if (_epoll < 0) return false;
    _events.resize(kEventsPerWait * sizeof(struct epoll_event));
    _ready.reserve(kEventsPerWait);
    _open = true;
    return true;
#else
    return false;
#endif
}

bool MqttPollSet::add(int handle, uint64_t token, bool wantWrite) {
#if defined(MQ_HAVE_EPOLL)
    if (_epoll < 0 || handle < 0) return false;
    struct epoll_event event;
    std::memset(&event, 0, sizeof event);
    event.events = EPOLLIN | EPOLLRDHUP | (wantWrite ? (uint32_t)EPOLLOUT : 0u);
    event.data.u64 = token;
    return epoll_ctl(_epoll, EPOLL_CTL_ADD, handle, &event) == 0;
#else
    (void)handle; (void)token; (void)wantWrite;
    return false;
#endif
}

bool MqttPollSet::update(int handle, uint64_t token, bool wantWrite) {
#if defined(MQ_HAVE_EPOLL)
    if (_epoll < 0 || handle < 0) return false;
    struct epoll_event event;
    std::memset(&event, 0, sizeof event);
    event.events = EPOLLIN | EPOLLRDHUP | (wantWrite ? (uint32_t)EPOLLOUT : 0u);
    event.data.u64 = token;
    return epoll_ctl(_epoll, EPOLL_CTL_MOD, handle, &event) == 0;
#else
    (void)handle; (void)token; (void)wantWrite;
    return false;
#endif
}

void MqttPollSet::remove(int handle) {
#if defined(MQ_HAVE_EPOLL)
    if (_epoll < 0 || handle < 0) return;
    // The event argument is ignored for DEL but must be non-null before
    // Linux 2.6.9; passing one costs nothing.
    struct epoll_event event;
    std::memset(&event, 0, sizeof event);
    epoll_ctl(_epoll, EPOLL_CTL_DEL, handle, &event);
#else
    (void)handle;
#endif
}

bool MqttPollSet::isOpen() const { return _open; }

void MqttPollSet::clear() { _watched.clear(); }

void MqttPollSet::watch(int handle, bool forWriting) {
    if (handle < 0) return;
    Watched entry;
    entry.handle = handle;
    entry.forWriting = forWriting;
    entry.ready = false;
    _watched.push_back(entry);
}

int MqttPollSet::wait(int timeoutMs) {
#if defined(MQ_HAVE_EPOLL)
    if (_epoll >= 0) {
        _ready.clear();
        _fresh = true;
        struct epoll_event* events = (struct epoll_event*)_events.data();
        const int count = epoll_wait(_epoll, events, (int)kEventsPerWait, timeoutMs);
        if (count <= 0) return 0;
        for (int index = 0; index < count; index++) {
            // EPOLLERR and EPOLLHUP are readiness, for the same reason POLLERR
            // is below: a dead socket has something to say.
            _ready.push_back(events[index].data.u64);
        }
        return count;
    }
#endif
    for (size_t index = 0; index < _watched.size(); index++) _watched[index].ready = false;
    if (_watched.empty()) return 0;

    const size_t needed = _watched.size() * sizeof(MqPollFd);
    if (_fds.size() != needed) _fds.resize(needed);
    MqPollFd* fds = (MqPollFd*)_fds.data();

    for (size_t index = 0; index < _watched.size(); index++) {
        std::memset(&fds[index], 0, sizeof(MqPollFd));
        fds[index].fd = _watched[index].handle;
        fds[index].events = POLLIN;
        if (_watched[index].forWriting) fds[index].events |= POLLOUT;
    }

    const int ready = MQ_POLL(fds, (unsigned)_watched.size(), timeoutMs);
    if (ready <= 0) return 0;

    for (size_t index = 0; index < _watched.size(); index++) {
        const MqPollFd& entry = fds[index];
        // POLLERR and POLLHUP ARE READINESS. The connection has something to
        // say and what it has to say is that it is over; calling that "not
        // ready" would leave a dead socket unserviced for ever.
        if (entry.revents != 0) _watched[index].ready = true;
    }
    return ready;
}

bool MqttPollSet::isReady(int handle) const {
    // Persistent mode answers by token, not by descriptor; a caller asking
    // this question there gets the safe answer.
    if (_epoll >= 0) return true;
    if (handle < 0) return false;
    for (size_t index = 0; index < _watched.size(); index++) {
        if (_watched[index].handle == handle) return _watched[index].ready;
    }
    // NOT WATCHED MEANS SERVICE IT ANYWAY. A caller that asks about a
    // descriptor this set was never told about must not be told "idle" - it
    // would stop servicing a connection for ever. Unknown is "yes".
    return true;
}

#endif  // MQTT_PLATFORM_HOST
