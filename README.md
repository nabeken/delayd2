# delayd2

[![Build Status](https://travis-ci.org/nabeken/delayd2.svg?branch=sqs)](https://travis-ci.org/nabeken/delayd2)
[![BSD License](http://img.shields.io/badge/license-BSD-blue.svg)](https://github.com/nabeken/delayd2/blob/master/LICENSE)

delayd2 is an available `setTimeout()` service for scheduling message sends.

delayd2 stores scheduling information on clustered (aka streaming replication) PostgreSQL instances as a stable storage layer. MySQL (or Amazon Aurora) support is also planned.

# Message Format

Message bodies are forwarded unchanged from received messages after their delay elapses.

All Delayd directives are taken from
[SQS Message Attributes](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSMessageAttributes.html).

### Required Message Attributes

- delayd2-delay (Integer) Delay time in ms before emitting this message.
- delayd2-target (string) Target exchange for this message.

## Acknowledgement and License

delayd2 is my whole rework against [nabeken/delayd](https://github.com/nabeken/delayd) which was an fork of [goinstant/delayd](https://github.com/goinstant/delayd).

&copy; 2015 Ken-ichi TANABE. Licensed under the BSD 3-clause license.
