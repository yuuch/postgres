#!/bin/bash
# 1. Ensure postgres is running
../installed/bin/pg_ctl -D ~/data/pg_tpch -o "-p 55442" restart
sleep 2

# 2. Open a psql connection in the background and get its PID
# We use a FIFO to keep the psql session open
rm -f /tmp/psql_fifo
mkfifo /tmp/psql_fifo
../installed/bin/psql -p 55442 -d tpch < /tmp/psql_fifo > /tmp/psql_out &
PSQL_PID=$!

# 3. Get the backend PID for this connection
# We ask psql to tell us its backend PID
echo "SELECT pg_backend_pid();" > /tmp/psql_fifo
sleep 1
BACKEND_PID=$(grep -o '[0-9]\+' /tmp/psql_out | tail -n 1)

if [ -z "$BACKEND_PID" ]; then
    echo "Failed to get backend PID"
    kill $PSQL_PID
    exit 1
fi

echo "Backend PID is $BACKEND_PID"

# 4. Run lldb attached to the backend in the background
# It will wait for the crash and then print the backtrace
lldb -p $BACKEND_PID --batch \
    -o "continue" \
    -o "bt" \
    -o "quit" > /tmp/lldb_bt.txt 2>&1 &
LLDB_PID=$!

sleep 2

# 5. Send the crashing query to the psql session
echo "LOAD 'llvmjit';" > /tmp/psql_fifo
echo "LOAD 'pg_volvec';" > /tmp/psql_fifo
echo "SET pg_volvec.enabled = on;" > /tmp/psql_fifo
echo "SELECT sum(l_quantity) FROM lineitem;" > /tmp/psql_fifo

# 6. Wait for things to settle
sleep 5

echo "Analysis complete. Checking /tmp/lldb_bt.txt"
cat /tmp/lldb_bt.txt

# Cleanup
kill $PSQL_PID 2>/dev/null
rm /tmp/psql_fifo
