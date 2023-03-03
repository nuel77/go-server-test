package main

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

type SnsClient struct {
	inner    *sns.SNS
	TopicArn string
}

func NewSnsClient(sess *session.Session) *SnsClient {
	return &SnsClient{
		inner:    sns.New(sess),
		TopicArn: SnsTopicArn,
	}
}

func (c *SnsClient) PublishToSns(message string) error {
	_, err := c.inner.Publish(&sns.PublishInput{
		Message:  &message,
		TopicArn: &c.TopicArn,
	})
	return err
}
