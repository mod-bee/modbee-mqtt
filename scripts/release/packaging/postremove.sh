#!/bin/sh
# After modbee's package is removed. The modbee user and /var/lib/modbee -
# the logins, sessions and retained messages - are kept: removing the program
# is not deleting a plant's data.
set -e

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi
exit 0
