#!/usr/bin/env bash
set -e
# quick demo: enqueue three jobs, start 2 workers, show status, wait, show DLQ etc.

# remove DB to start clean
rm -f queuectl.db
python3 queuectl.py enqueue '{"id":"job-ok","command":"echo ok && sleep 1","max_retries":2}'
python3 queuectl.py enqueue '{"id":"job-fail","command":"bash -c \"exit 2\"","max_retries":2}'
python3 queuectl.py enqueue '{"id":"job-error","command":"nonexistent_command_xyz","max_retries":1}'
python3 queuectl.py config set backoff_base 2
python3 queuectl.py worker start --count 2
echo "workers started"
sleep 6
echo "status after 6s:"
python3 queuectl.py status
echo "list all jobs:"
python3 queuectl.py list
echo "DLQ:"
python3 queuectl.py dlq list
python3 queuectl.py worker stop
echo "requested workers stop"
