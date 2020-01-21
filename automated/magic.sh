set -euo pipefail

if ! command -v docker-compose > /dev/null; then
    echo "docker-compose doesn't seem to be install. Please install it and make sure its available in PATH."
    exit 1;
fi

# verify config
grep HOST_DATASET_PATH ./.env

read -p "Is dataset path correct? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$  ]]; then
    echo "Please set the correct path in the .env file and run this script again."
    exit 2
fi


# Run the imports
docker-compose up -d --build && docker-compose logs -f driver

# Load mapreduce
cd ../processing/mapreduce
echo $PWD
./compile_docker.sh
cd -
echo $PWD
