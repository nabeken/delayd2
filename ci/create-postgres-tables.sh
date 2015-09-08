#!/bin/bash
WORKSPACE=$(git rev-parse --show-toplevel)

cd "${WORKSPACE}"
createdb delayd2-test
psql delayd2-test < schema/pq/delayd2.sql
