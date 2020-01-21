# detect WSL :)
if [[ $PWD == /mnt/[cd]/* ]]; then
    MOUNTABLE_PWD=$(sed 's|/mnt/|/|' <<< $PWD)
else 
    MOUNTABLE_PWD=$PWD
fi

docker run --rm -v "$MOUNTABLE_PWD":/usr/src/mapred -w /usr/src/mapred openjdk:8-alpine ash -c "./compile.sh"
