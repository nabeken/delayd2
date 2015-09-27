FROM golang:1.5-onbuild
MAINTAINER TANABE Ken-ichi <nabeken@tknetworks.org>

RUN useradd -m delayd2 && \
  chown delayd2:delayd2 -R /go

USER delayd2
