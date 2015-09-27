FROM golang:1.5
MAINTAINER TANABE Ken-ichi <nabeken@tknetworks.org>

RUN mkdir -p /go/src/github.com/nabeken/delayd2
WORKDIR /go/src/github.com/nabeken/delayd2

COPY . /go/src/github.com/nabeken/delayd2

RUN go-wrapper download -d \
  github.com/aws/aws-sdk-go \
  github.com/hashicorp/errwrap \
  github.com/hashicorp/go-multierror \
  github.com/kelseyhightower/envconfig \
  github.com/lib/pq \
  github.com/mitchellh/cli \
  github.com/nabeken/aws-go-sqs/... \
  github.com/vaughan0/go-ini \
  golang.org/x/crypto/...

RUN go-wrapper download ./... && \
  go install ./...

RUN useradd -m delayd2 && \
  chown delayd2:delayd2 -R /go

USER delayd2

ENTRYPOINT ["delayd2"]
