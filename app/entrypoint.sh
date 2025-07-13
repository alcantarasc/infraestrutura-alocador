#!/bin/sh
# Ativa o virtualenv
. /venv/bin/activate

# Start supervisor
supervisord -c /app/supervisord.conf 