// A broker's own list of who may connect.
//
// MqttBroker::onAuthorise() is the general mechanism - any policy at all can be
// written as a hook. This is the policy almost every site actually wants: a
// short list of user names and passwords, one per device, checked on CONNECT.
// It is a ready-made hook rather than something each application writes,
// because the two ways to get it wrong are easy and silent:
//
//   - comparing with strcmp(), which stops at the first NUL. A password is
//     Binary Data at both protocol versions [V5:3.1.3.5], so a password that
//     shares a prefix with the right one up to a zero byte would be admitted.
//   - comparing with an early exit, which answers faster the more of a guess
//     is right. Both comparisons here run over the whole stored value whatever
//     the offered one contains.
//
// FIXED SIZE, ALLOCATED NOWHERE. A broker on a board is configured once and
// runs for months, and an account table that could grow is a heap that could
// fragment. MQ_BROKER_MAX_ACCOUNTS raises the limit for a build that needs it.
//
// WHEN ACCOUNTS ARE REQUIRED. Declaring any account makes them required for
// every CONNECT - including declaring one that is disabled. The alternative,
// "anonymous until an account is enabled", means switching off the last
// account quietly opens the broker to anybody, which is exactly backwards. An
// empty table leaves the broker as it was configured: MqttBrokerConfig's
// allowAnonymous decides, as it always has.
//
// Topic permissions are not here. This answers who may CONNECT; what a
// connected client may publish or subscribe to is still onAuthorise()'s to
// decide, and a hook that needs both can call authorise() from its own.

#ifndef MODBEE_MQTT_BROKER_ACCOUNTS_H
#define MODBEE_MQTT_BROKER_ACCOUNTS_H

#include "MqttBroker.h"

#include <stddef.h>
#include <stdint.h>

#ifndef MQ_BROKER_MAX_ACCOUNTS
#define MQ_BROKER_MAX_ACCOUNTS 16
#endif

// 64, because that is what an OpenPLC STRING pin can carry and what a user
// name or password typed into a PLC program realistically is. Longer is refused
// at set() rather than truncated: a truncated password is a different password.
#ifndef MQ_BROKER_ACCOUNT_FIELD_BYTES
#define MQ_BROKER_ACCOUNT_FIELD_BYTES 64
#endif

class MqttBrokerAccounts {
public:
    // Add an account, or replace the password and enabled flag of one that
    // already exists. False if the name is empty, either field is too long, or
    // the table is full.
    //
    // A disabled account still makes accounts REQUIRED - see the header
    // comment - and simply cannot log in.
    bool set(const char* userName, const uint8_t* password, size_t passwordLength,
             bool enabled = true);
    bool set(const char* userName, const char* password, bool enabled = true);

    bool remove(const char* userName);
    void clear();

    size_t count() const { return _count; }
    bool required() const { return _count > 0; }

    // The decision, for a CONNECT. Anything else is allowed: topic policy is
    // not this class's question. `reasonCode` is written only on a refusal.
    bool authorise(const MqttBrokerRequest& request, uint8_t& reasonCode);

    // Ready to hand to MqttBroker::onAuthorise(&MqttBrokerAccounts::hook, this).
    static bool hook(void* context, const MqttBrokerRequest& request,
                     uint8_t& reasonCode);

    // --- What happened, per account and overall ---
    //
    // Per account so a panel can show WHICH device is failing to log in - the
    // whole reason to give each device its own account.
    uint32_t logins(const char* userName) const;
    uint32_t refusals(const char* userName) const;

    uint32_t accepted() const { return _accepted; }
    uint32_t refusedUnknown() const { return _refusedUnknown; }
    uint32_t refusedAnonymous() const { return _refusedAnonymous; }
    uint32_t refusedPassword() const { return _refusedPassword; }
    uint32_t refusedDisabled() const { return _refusedDisabled; }

private:
    struct Account {
        bool inUse = false;
        bool enabled = false;
        uint8_t nameLength = 0;
        uint8_t passwordLength = 0;
        char name[MQ_BROKER_ACCOUNT_FIELD_BYTES];
        uint8_t password[MQ_BROKER_ACCOUNT_FIELD_BYTES];
        uint32_t logins = 0;
        uint32_t refusals = 0;
    };

    int find(const char* userName) const;
    static void wipe(Account& account);

    Account _accounts[MQ_BROKER_MAX_ACCOUNTS];
    size_t _count = 0;

    uint32_t _accepted = 0;
    uint32_t _refusedUnknown = 0;
    uint32_t _refusedAnonymous = 0;
    uint32_t _refusedPassword = 0;
    uint32_t _refusedDisabled = 0;
};

#endif
