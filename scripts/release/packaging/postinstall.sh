#!/bin/sh
# After modbee's package is installed or upgraded (.deb: "configure";
# .rpm: 1 on install, 2 on upgrade).
set -e

if ! getent group modbee >/dev/null; then
    groupadd --system modbee
fi
if ! getent passwd modbee >/dev/null; then
    nologin=$(command -v nologin || echo /bin/false)
    useradd --system --gid modbee --home-dir /var/lib/modbee --no-create-home \
        --shell "$nologin" --comment "ModBee MQTT" modbee
fi
mkdir -p /var/lib/modbee
chown modbee:modbee /var/lib/modbee
chmod 0750 /var/lib/modbee

# The admin password (modbee.yaml: password-file): chosen here, once, if
# there is none - an upgrade keeps the one there is. Readable by root and the
# modbee service only.
secret=/etc/modbee/admin-password
fresh=
if [ ! -s "$secret" ]; then
    (umask 077; head -c 18 /dev/urandom | base64 | tr -d '/+=\n' | head -c 24 > "$secret")
    fresh=yes
fi
chown root:modbee "$secret"
chmod 0640 "$secret"

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
    # An upgrade restarts a running broker on the new binary; a first install
    # leaves starting it to you.
    systemctl try-restart modbee.service || true
fi

if [ -n "$fresh" ]; then
    cat <<EOF

ModBee MQTT is installed. To start it, now and at every boot:

    sudo systemctl enable --now modbee

Its settings:     /etc/modbee/modbee.yaml
The admin login:  admin, password in /etc/modbee/admin-password
Its data:         /var/lib/modbee

EOF
fi
exit 0
