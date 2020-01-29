#!/bin/bash
set -euo pipefail
DATASET_DIR=$1
OUT_DIR=$2
TARGET_SIZE=$3

USAGE="usage: ./prepare_dataset DATASET_DIR OUTPUT_DIR TARGET_SIZE"

if [[ ! $# -eq 3 ]]; then
    echo $USAGE
    exit 1;
fi  

echo "Generating dataset truncated to $TARGET_SIZE records"
echo "Source dataset: '$1'"
echo "Output directory: '$2'"
# read -p "Are both paths correct? (y/n) " -n 1 -r
# echo

# if [[ ! $REPLY =~ ^[Yy]$  ]]; then
#     echo $USAGE
#     exit 2
# fi

if [[ -d $OUT_DIR || -e $OUT_DIR ]]; then
    echo "$OUT_DIR already exists"
    exit 3
fi

mkdir -p $OUT_DIR

for f in $DATASET_DIR/*.json; do
    IN_FILE=$(readlink -f "$f")
    FILENAME=$(basename "$IN_FILE")
    OUT_FILE="$OUT_DIR/$FILENAME"
    echo "processing: $IN_FILE -> $OUT_FILE..."
    head -n $TARGET_SIZE "$IN_FILE" > "$OUT_DIR/$FILENAME"
done
echo "copying CSV directory..."
cp -r "$DATASET_DIR/CSV" "$OUT_DIR"
echo "Done."


