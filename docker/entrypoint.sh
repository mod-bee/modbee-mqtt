#!/bin/sh
# The image's entrypoint. With no arguments, or only options, it runs the
# broker on /etc/modbee/modbee.yaml:
#
#   docker run ghcr.io/mod-bee/modbee-mqtt                       the broker
#   docker run ghcr.io/mod-bee/modbee-mqtt --metrics-port 9464   the broker, plus metrics
#
# Given a command - any first word that is not an option - it runs that
# instead, so the image is also the command-line tool, the Application API's
# commands included:
#
#   docker run --rm ghcr.io/mod-bee/modbee-mqtt pub -h broker.example.com -t test -m hi
#   docker exec -e MODBEE_API=unix:/run/modbee/api.sock broker modbee-mqtt connections list
#
# exec, so the broker is PID 1 and receives docker stop's SIGTERM itself.
case "$1" in
    modbee-mqtt|modbee) shift; exec modbee-mqtt "$@" ;;
    --help|--version) exec modbee-mqtt "$@" ;;
    ""|-*) ;;
    *) exec modbee-mqtt "$@" ;;
esac
exec modbee-mqtt serve -C /etc/modbee/modbee.yaml "$@"
