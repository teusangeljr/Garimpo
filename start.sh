#!/usr/bin/env bash
set -e

echo "============================="
echo " Garimpo Backend Startup"
echo " $(date)"
echo "============================="
echo "Broker: Threading nativo (sem Redis/Celery)"

# Inicia a API do Flask em foreground
echo "--- Iniciando Gunicorn (API)..."
exec gunicorn app:app --timeout 300 --workers 1 --bind 0.0.0.0:${PORT:-5000}
