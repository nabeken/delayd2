package delayd2

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
)

func TestExtractDelayd2MessageAttributes(t *testing.T) {
	assert := assert.New(t)

	expectedDuration := int64(60)
	expectedRelayTo := "relay-to"

	for i, tc := range []struct {
		IsErr    bool
		Messages map[string]*sqs.MessageAttributeValue
	}{
		{
			Messages: map[string]*sqs.MessageAttributeValue{
				sqsMessageDurationKey: &sqs.MessageAttributeValue{
					DataType:    aws.String("Number"),
					StringValue: aws.String("60"),
				},
				sqsMessageRelayToKey: &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(expectedRelayTo),
				},
			},
		},
		{
			IsErr: true,
			Messages: map[string]*sqs.MessageAttributeValue{
				sqsMessageDurationKey: &sqs.MessageAttributeValue{
					DataType:    aws.String("Number"),
					StringValue: aws.String("60"),
				},
			},
		},
		{
			IsErr: true,
			Messages: map[string]*sqs.MessageAttributeValue{
				sqsMessageDurationKey: &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String("60"),
				},
			},
		},
		{
			IsErr: true,
			Messages: map[string]*sqs.MessageAttributeValue{
				sqsMessageRelayToKey: &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String("relay-to"),
				},
			},
		},
		{
			IsErr: true,
			Messages: map[string]*sqs.MessageAttributeValue{
				sqsMessageRelayToKey: &sqs.MessageAttributeValue{
					DataType:    aws.String("Number"),
					StringValue: aws.String("relay-to"),
				},
			},
		},
	} {
		duration, relayTo, err := extractDelayd2MessageAttributes(&sqs.Message{
			MessageAttributes: tc.Messages,
		})
		if tc.IsErr {
			assert.Equal(ErrInvalidAttributes, err, "#%d failed", i+1)
		} else {
			assert.NoError(err, "#%d failed", i+1)
			assert.Equal(expectedDuration, duration, "#%d failed", i+1)
			assert.Equal(expectedRelayTo, relayTo, "#%d failed", i+1)
		}
	}
}
