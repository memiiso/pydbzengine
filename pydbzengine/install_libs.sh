#!/usr/bin/env bash

CURRENT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd ${CURRENT_DIR}
echo "Cleaning up pydbzengine/debezium/libs..."
rm -rf "${CURRENT_DIR}/debezium/libs"
mkdir -p "${CURRENT_DIR}/debezium/libs"
mvn dependency:copy-dependencies -DoutputDirectory="${CURRENT_DIR}/debezium/libs"