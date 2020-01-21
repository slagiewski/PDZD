set -euo pipefail

if ! command -v docker-compose > /dev/null; then
    echo "docker-compose doesn't seem to be install. Please install it and make sure its available in PATH."
    exit 1;
fi

# make relative paths work no matter what directory the script is run from by using the script's location as an anchor
SCRIPT_DIR=$(readlink -f $0 | xargs dirname)
echo "SCRIPT_DIR: '$SCRIPT_DIR'"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yaml"

# verify config
grep HOST_DATASET_PATH "$SCRIPT_DIR/.env"

read -p "Is dataset path correct? (y/n) " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$  ]]; then
    echo "Please set the correct path in the .env file ('$SCRIPT_DIR/.env') and run this script again."
    exit 2
fi

# RUN

mkdir -p $SCRIPT_DIR/jars/mapreduce

# Prepare mapreduce job jars
echo "compiling mapreduce jars"
cd "$SCRIPT_DIR/../processing/mapreduce"
./compile_docker.sh
cp out/*.jar "$SCRIPT_DIR/jars/mapreduce"
cd -
export HOST_JARS_PATH=$(sed 's|^/mnt/|/|g' <<< "$SCRIPT_DIR/jars")

# Run the imports
docker-compose -f $COMPOSE_FILE --env-file "$SCRIPT_DIR/.env" up -d --build && docker-compose -f $COMPOSE_FILE logs -f driver

###############
# MAP REDUCE
###############

echo "MapReduce jobs found:"
docker-compose -f $COMPOSE_FILE exec namenode ls /host_jars/mapreduce
echo "Running photosadvanced.jar"
echo "Preparing input and output dirs..."
docker-compose -f $COMPOSE_FILE exec namenode hadoop fs -mkdir -p /user/Analysis2/MapReduce/input
docker-compose -f $COMPOSE_FILE exec namenode hadoop fs -mkdir -p /user/Analysis2/MapReduce/output
docker-compose -f $COMPOSE_FILE exec namenode hadoop fs -cp /user/DataSources/CSV/photo/photo.csv /user/Analysis2/MapReduce/input
docker-compose -f $COMPOSE_FILE exec namenode hadoop fs -rmr /user/Analysis2/MapReduce/output
echo "Running MapReduce job..."
docker-compose -f $COMPOSE_FILE exec namenode hadoop jar /host_jars/mapreduce/photosadvanced.jar PhotosAdvanced /user/Analysis2/MapReduce/input /user/Analysis2/MapReduce/output
docker-compose -f $COMPOSE_FILE exec namenode hadoop fs -cp -f /user/Analysis2/MapReduce/output/part-r-00000 /user/Analysis2/middle_set_photos_count.csv
