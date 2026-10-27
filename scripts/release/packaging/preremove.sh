#!/bin/sh
# Before modbee's package is removed (.deb: "remove"; .rpm: 0). Not on an
# upgrade (.deb: "upgrade"; .rpm: 1) - postinstall restarts it then.
set -e

case "$1" in
    remove|0)
        if [ -d /run/systemd/system ]; then
            systemctl disable --now modbee.service >/dev/null 2>&1 || true
        fi
        ;;
esac
exit 0
