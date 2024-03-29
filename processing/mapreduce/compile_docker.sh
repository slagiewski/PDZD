# this script runs ./compile.sh in a docker container with proper java version to avoid requiring jdk installation on the host

# detect WSL :)
if [[ $PWD == /mnt/[cd]/* ]]; then
    MOUNTABLE_PWD=$(sed 's|/mnt/|/|' <<< $PWD)
else 
    MOUNTABLE_PWD=$PWD
fi

docker run --rm -v "$MOUNTABLE_PWD":/usr/src/mapred -w /usr/src/mapred openjdk:8-alpine ash -c "./compile.sh"
