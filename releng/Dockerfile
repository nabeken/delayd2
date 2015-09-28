# golang:1.5 is based on debian:jessie
FROM debian:jessie
MAINTAINER TANABE Ken-ichi <nabeken@tknetworks.org>

RUN apt-get update && \
  apt-get install -y --no-install-recommends ca-certificates && \
  rm -rf /var/lib/apt/

COPY bin /usr/local/bin/

RUN useradd -m delayd2

USER delayd2
ENTRYPOINT ["delayd2"]
