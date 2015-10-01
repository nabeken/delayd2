#!/bin/bash
WORKSPACE=$(git rev-parse --show-toplevel)

set -e
set -x

cd "${WORKSPACE}"/releng

rm -rf bin schema || :
docker pull nabeken/delayd2:latest
docker run -it --rm --entrypoint sh nabeken/delayd2:latest -c 'tar -C /go -cf - bin | base64' | openssl enc -d -base64 | tar -xvf -
docker run -it --rm --entrypoint sh nabeken/delayd2:latest -c 'tar -C /go/src/github.com/nabeken/delayd2 -cf - schema | base64' | openssl enc -d -base64 | tar -xvf -

docker build --no-cache -t nabeken/delayd2-release:latest .

docker images nabeken/delayd2-release
