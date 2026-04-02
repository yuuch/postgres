#!/bin/bash
# Clean up
rm -f /tmp/crash_bt.txt

# Start DB
../installed/bin/pg_ctl -D ~/data/pg_tpch -o "-p 55442" restart
sleep 2

# We will use lldb to WAIT for any postgres process to crash
# and automatically print the backtrace.
# -w waits for process by name
# --one-line "continue" starts it
# -o "bt" runs on stop
echo "Starting LLDB in wait-and-catch mode..."
(lldb --batch -n postgres -o "continue" -o "bt" -o "quit" > /tmp/crash_bt.txt 2>&1) &
LLDB_PID=$!
sleep 2

echo "Running the crashing query..."
../installed/bin/psql -p 55442 -d tpch -c "
SET max_parallel_workers_per_gather = 0;
LOAD 'llvmjit'; 
LOAD 'pg_volvec'; 
SET pg_volvec.enabled = on; 
SELECT sum(l_quantity) FROM lineitem;
"

sleep 3
echo "--- Backtrace ---"
cat /tmp/crash_bt.txt
kill $LLDB_PID 2>/dev/null
