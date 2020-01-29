#!/bin/bash

set -euo pipefail

SIZE=$2

DATASET_DIR=$(sed 's|/$||' <<< $1 | xargs -I{} readlink -f {})
TEST_DATASET_DIR=$(readlink -f "${DATASET_DIR}_$SIZE")
TEST_DATASET_DIR_FOR_COMPOSE=$(sed 's|/mnt||' <<< $TEST_DATASET_DIR)
echo $TEST_DATASET_DIR
echo $TEST_DATASET_DIR_FOR_COMPOSE

SCRIPT_DIR=$(readlink -f $0 | xargs dirname)
COMPOSE_FILE="$SCRIPT_DIR/../automated/docker-compose.yaml"
"$SCRIPT_DIR/prepare_dataset.sh" "$DATASET_DIR" $TEST_DATASET_DIR $SIZE

sed "s|{{HDFS_DATASET_PATH}}|$TEST_DATASET_DIR_FOR_COMPOSE|" ../automated/.env.template > "$SCRIPT_DIR/test.env"

echo "Clearing out the environment"
docker-compose -f $COMPOSE_FILE --env-file "$SCRIPT_DIR/../automated/.env" down
docker volume rm automated_namenode automated_datanode || true
echo "Backing up original .env file..."
cp "$SCRIPT_DIR/../automated/.env" "$SCRIPT_DIR/../automated/.env.bkp"
echo "Creating new .env file..."
cp "$SCRIPT_DIR/test.env" "$SCRIPT_DIR/../automated/.env"

echo "Running the test"
time "$SCRIPT_DIR/../automated/magic.sh"

echo "Cleaning up"
rm -r "$TEST_DATASET_DIR"
mv "$SCRIPT_DIR/../automated/.env.bkp" "$SCRIPT_DIR/../automated/.env"
