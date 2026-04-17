#!/bin/bash
# Start the query in the background
../installed/bin/psql -p 55442 -d tpch -f pg_volvec/test_q1_10g.sql > /tmp/q1_profile.out &
PSQL_PID=$!

# Wait a bit for the backend to start working
sleep 2

# Find the backend process handling the query (the one with highest CPU or most recent)
BACKEND_PID=$(ps aux | grep "postgres: chenyunwen tpch" | grep -v grep | awk '{print $2}' | tail -n 1)

if [ -z "$BACKEND_PID" ]; then
    echo "Could not find backend PID"
    kill $PSQL_PID
    exit 1
fi

echo "Sampling backend PID: $BACKEND_PID"
sample $BACKEND_PID 5 -f /tmp/q1_sample.txt

echo "Sample complete. Output in /tmp/q1_sample.txt"
wait $PSQL_PID
