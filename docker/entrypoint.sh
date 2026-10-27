#!/bin/sh
# The image's entrypoint. With no arguments, or only options, it runs the
# broker on /etc/modbee/modbee.yaml:
#
#   docker run ghcr.io/mod-bee/modbee-mqtt                       the broker
#   docker run ghcr.io/mod-bee/modbee-mqtt --metrics-port 9464   the broker, plus metrics
#
# Given one of modbee's commands, it runs that instead, so the image is also
# the command-line tool:
#
#   docker run --rm ghcr.io/mod-bee/modbee-mqtt pub -h broker.example.com -t test -m hi
#
# exec, so the broker is PID 1 and receives docker stop's SIGTERM itself.
case "$1" in
    pub|sub|req|bench|check|serve|user|key|config|provision|backup|restore|admin|help|--help|version|--version) exec modbee-mqtt "$@" ;;
    modbee-mqtt|modbee) shift; exec modbee-mqtt "$@" ;;
esac
exec modbee-mqtt serve -C /etc/modbee/modbee.yaml "$@"
