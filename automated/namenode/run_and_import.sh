# some extra safety, see https://vaneyckt.io/posts/safer_bash_scripts_with_set_euxo_pipefail/
set -euo pipefail

echo -e "\n[INITIALIZATION] Starting Hadoop namenode\n"
/run.sh &
echo -e "\n[INITIALIZATION] Waiting for namenode and datanode to come online\n"
/wait-for-it.sh -p 50070 -h localhost
/wait-for-it.sh -p 50075 -h datanode -t 45
echo -e "\n[INITIALIZATION] Hadoop is ready, starting initial import (if required)\n"

HADOOP_IMPORT_DIR=${CONFIG_HDFS_DATA_DIR:-/user/DataSources}

if ! hadoop fs -test -e $HADOOP_IMPORT_DIR; then
    echo -e "\n[INITIALIZATION] Creating data directory in HDFS: $HADOOP_IMPORT_DIR\n"
    hadoop fs -mkdir -p $HADOOP_IMPORT_DIR
fi

cd /_host_data/

for f in $(find -type f | sed 's_^\./__'); do
    HDFS_PATH="$HADOOP_IMPORT_DIR/$f"
    if hadoop fs -test -e "$HDFS_PATH"; then
        echo -e "\n[INITIALIZATION] $f: already imported (found in HDFS at: $HDFS_PATH)\n"
    else
        echo -e "\n[INITIALIZATION] $f: importing to $HDFS_PATH\n"
        if hadoop fs -put "$f" "$HDFS_PATH"; then
            echo -e "\n[INITIALIZATION] $f: import successful\n"
        else 
            echo -e "\n[INITIALIZATION] $f: import failed\n"
        fi
    fi
done
echo -e "\n[INITIALIZATION] All expected files are available\n"
hadoop fs -lsr $HADOOP_IMPORT_DIR
echo -e "\n[INITIALIZATION] Initialization finished\n"
# /run.sh that starts Haddoop runs in background, waiting for it prevents container from stopping
wait
