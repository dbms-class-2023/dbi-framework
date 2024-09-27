#!/bin/bash

set -e
DEFAULT_COST=$(./gradlew run --args='join-benchmark --cache-impl clock --cache-size 70 --data-scale 10 --join-algorithm merge' | grep "JOIN COST" | cut -d ":" -f 2)
echo "Join cost with the DEFAULT (SLOW) merge sort implementation: $DEFAULT_COST"

REAL_COST=$(./gradlew run --args='join-benchmark --cache-impl clock --cache-size 70 --data-scale 10 --join-algorithm merge --real-sort' | grep "JOIN COST" | cut -d ":" -f 2)
echo "Join cost with the SUBMITTED (hopefully FAST) merge sort implementation: $REAL_COST"

EXPR="${DEFAULT_COST}/10 < ${REAL_COST}"
echo "$EXPR ?"
if (($(bc <<< "$EXPR") == 1)); then
    echo "Yes. It appears that the SUBMITTED implementation may be improved!";
    exit 1;
fi