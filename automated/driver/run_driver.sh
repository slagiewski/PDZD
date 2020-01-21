#!/bin/bash
if ! /wait-for-it.sh -h hive-server -p 10000 -t 90; then
    echo "Waiting another 90s..."
    /wait-for-it.sh -h hive-server -p 10000 -t 90;
fi

python -u /app/main.py
bash
