#!/bin/sh

set -e
DEFAULT_COST=$(./gradlew run --args='join-benchmark --cache-impl clock --cache-size 70 --data-scale 10 --join-algorithm merge' | grep "JOIN COST" | cut -d ":" -f 2)
echo "Join cost with the DEFAULT (SLOW) merge sort implementation: $DEFAULT_COST"

REAL_COST=$(./gradlew run --args='join-benchmark --cache-impl clock --cache-size 70 --data-scale 10 --join-algorithm merge --real-sort' | grep "JOIN COST" | cut -d ":" -f 2)
echo "Join cost with the PROVIDED (hopefully FAST) merge sort implementation: $REAL_COST"