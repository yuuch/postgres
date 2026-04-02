#!/bin/bash
# 1. Start a persistent backend
rm -f /tmp/backend_pid.txt
(echo "SELECT pg_backend_pid(); SELECT pg_sleep(100);" | ../installed/bin/psql -p 55442 -d tpch -t > /tmp/backend_pid.txt) &
sleep 2

BACKEND_PID=$(head -n 1 /tmp/backend_pid.txt | tr -d '[:space:]')
echo "Persistent backend PID: $BACKEND_PID"

if [ -z "$BACKEND_PID" ]; then
    echo "Failed to get PID"
    exit 1
fi

# 2. Attach LLDB
echo "Attaching LLDB..."
(lldb -p $BACKEND_PID --batch -o "continue" -o "bt" -o "quit" > /tmp/crash_bt.txt 2>&1) &
sleep 2

# 3. Trigger crash in the SAME session? 
# No, I can't easily send more to the same session.
# But I can use ANOTHER session to run the query.
# Wait, the other session will have a DIFFERENT PID.

# I will use the LLDB "waitfor" feature on the next 'postgres' process.
(lldb --waitfor -n postgres --batch -o "continue" -o "bt" -o "quit" > /tmp/crash_bt.txt 2>&1) &
sleep 1
echo "Running query..."
../installed/bin/psql -p 55442 -d tpch -f pg_volvec/test_scan_power.sql

sleep 2
cat /tmp/crash_bt.txt
