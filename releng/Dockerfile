# golang:1.5 is based on debian:jessie
FROM debian:jessie
MAINTAINER TANABE Ken-ichi <nabeken@tknetworks.org>

ENV PATH /srv/delayd2/bin:$PATH

RUN apt-get update && \
  apt-get install -y --no-install-recommends ca-certificates && \
  rm -rf /var/lib/apt/

RUN mkdir -p /srv/delayd2/bin
COPY schema /srv/delayd2/schema
COPY bin /srv/delayd2/bin/

RUN useradd -m delayd2

WORKDIR /srv/delayd2
USER delayd2
