#include "MqttBrokerAccounts.h"

#include <string.h>

namespace {

// Equal, without stopping early.
//
// The loop always runs over the STORED length, so the time taken depends on
// the account and not on how much of a guess was right. A length mismatch is
// folded into the same accumulator rather than returned early, for the same
// reason.
bool sameBytes(const uint8_t* stored, size_t storedLength,
               const uint8_t* offered, size_t offeredLength) {
    uint32_t difference = (uint32_t)(storedLength ^ offeredLength);
    for (size_t index = 0; index < storedLength; index++) {
        const uint8_t theirs = index < offeredLength ? offered[index] : 0;
        difference |= (uint32_t)(stored[index] ^ theirs);
    }
    return difference == 0;
}

}  // namespace

bool MqttBrokerAccounts::set(const char* userName, const uint8_t* password,
                             size_t passwordLength, bool enabled) {
    if (userName == nullptr) return false;
    const size_t nameLength = strlen(userName);
    // Empty names are refused: a CONNECT with no user name is the anonymous
    // case, and an account with no name would make "anonymous" a password.
    if (nameLength == 0 || nameLength > MQ_BROKER_ACCOUNT_FIELD_BYTES) return false;
    if (passwordLength > MQ_BROKER_ACCOUNT_FIELD_BYTES) return false;
    if (password == nullptr && passwordLength > 0) return false;

    int slot = find(userName);
    if (slot < 0) {
        for (size_t index = 0; index < MQ_BROKER_MAX_ACCOUNTS; index++) {
            if (!_accounts[index].inUse) {
                slot = (int)index;
                break;
            }
        }
        if (slot < 0) return false;
        Account& fresh = _accounts[slot];
        fresh = Account();
        fresh.inUse = true;
        fresh.nameLength = (uint8_t)nameLength;
        memcpy(fresh.name, userName, nameLength);
        _count++;
    }

    Account& account = _accounts[slot];
    account.enabled = enabled;
    account.passwordLength = (uint8_t)passwordLength;
    if (passwordLength > 0) memcpy(account.password, password, passwordLength);
    // The tail is cleared rather than left holding the previous password: a
    // shorter replacement must not leave the end of the old one in memory.
    memset(account.password + passwordLength, 0,
           sizeof account.password - passwordLength);
    return true;
}

bool MqttBrokerAccounts::set(const char* userName, const char* password,
                             bool enabled) {
    const size_t length = password != nullptr ? strlen(password) : 0;
    return set(userName, (const uint8_t*)password, length, enabled);
}

bool MqttBrokerAccounts::remove(const char* userName) {
    const int slot = find(userName);
    if (slot < 0) return false;
    wipe(_accounts[slot]);
    _count--;
    return true;
}

void MqttBrokerAccounts::clear() {
    for (size_t index = 0; index < MQ_BROKER_MAX_ACCOUNTS; index++) {
        wipe(_accounts[index]);
    }
    _count = 0;
}

// Wiped, not just marked free: this held a password. The two arrays are
// cleared by hand rather than memset over the whole struct, which a compiler
// building with -Wclass-memaccess rightly objects to on a type with default
// member initialisers.
void MqttBrokerAccounts::wipe(Account& account) {
    memset(account.name, 0, sizeof account.name);
    memset(account.password, 0, sizeof account.password);
    account = Account();
}

int MqttBrokerAccounts::find(const char* userName) const {
    if (userName == nullptr) return -1;
    const size_t length = strlen(userName);
    for (size_t index = 0; index < MQ_BROKER_MAX_ACCOUNTS; index++) {
        const Account& account = _accounts[index];
        if (!account.inUse || account.nameLength != length) continue;
        if (memcmp(account.name, userName, length) == 0) return (int)index;
    }
    return -1;
}

bool MqttBrokerAccounts::authorise(const MqttBrokerRequest& request,
                                   uint8_t& reasonCode) {
    if (request.action != MqttBrokerAction::Connect) return true;

    // No accounts declared: this table has no opinion, and the broker's own
    // allowAnonymous has already been applied before the hook was called.
    if (_count == 0) return true;

    // 0x86, not 0x87, for every refusal below - an operator reading a log or a
    // PLC reading STATUS needs to know it was the CREDENTIAL. The broker uses
    // the same code for its own allowAnonymous refusal, so the two read alike.
    if (request.userName.length == 0) {
        _refusedAnonymous++;
        reasonCode = MQ_REASON_BAD_USER_NAME_OR_PASSWORD;
        return false;
    }

    // EVERY account is compared, and the match is remembered rather than
    // returned on: a scan that stopped at the right name would answer sooner
    // for names that exist, which tells a guesser which ones do.
    int match = -1;
    for (size_t index = 0; index < MQ_BROKER_MAX_ACCOUNTS; index++) {
        const Account& account = _accounts[index];
        if (!account.inUse) continue;
        const bool same = sameBytes((const uint8_t*)account.name,
                                    account.nameLength, request.userName.data,
                                    request.userName.length);
        if (same && match < 0) match = (int)index;
    }

    if (match < 0) {
        _refusedUnknown++;
        reasonCode = MQ_REASON_BAD_USER_NAME_OR_PASSWORD;
        return false;
    }

    Account& account = _accounts[match];

    // The password is compared even for a disabled account, so a disabled one
    // takes as long to refuse as a wrong password does.
    const bool right = sameBytes(account.password, account.passwordLength,
                                 request.password.data, request.password.length);

    if (!account.enabled) {
        account.refusals++;
        _refusedDisabled++;
        reasonCode = MQ_REASON_BAD_USER_NAME_OR_PASSWORD;
        return false;
    }
    if (!right) {
        account.refusals++;
        _refusedPassword++;
        reasonCode = MQ_REASON_BAD_USER_NAME_OR_PASSWORD;
        return false;
    }

    account.logins++;
    _accepted++;
    return true;
}

bool MqttBrokerAccounts::hook(void* context, const MqttBrokerRequest& request,
                              uint8_t& reasonCode) {
    if (context == nullptr) return true;
    return static_cast<MqttBrokerAccounts*>(context)->authorise(request,
                                                                reasonCode);
}

uint32_t MqttBrokerAccounts::logins(const char* userName) const {
    const int slot = find(userName);
    return slot < 0 ? 0 : _accounts[slot].logins;
}

uint32_t MqttBrokerAccounts::refusals(const char* userName) const {
    const int slot = find(userName);
    return slot < 0 ? 0 : _accounts[slot].refusals;
}
