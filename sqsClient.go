package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SqsClient struct {
	client    *sqs.SQS
	queueName string
	queueUrl  string
	messageId string
}

func NewSqsClient(sess *session.Session, name string) *SqsClient {
	svc := sqs.New(sess)
	urlResult, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(SqsQueueName),
	})
	if err != nil {
		log.Fatal("error getting queue url: %s", err.Error())
	}
	return &SqsClient{
		client:    svc,
		queueName: name,
		queueUrl:  *urlResult.QueueUrl,
		messageId: "FROM:ENCLAVE",
	}
}

func (c *SqsClient) SendToQueue(stid string, message string) error {
	_, err := c.client.SendMessage(&sqs.SendMessageInput{
		MessageBody:            aws.String(message),
		QueueUrl:               &c.queueUrl,
		MessageDeduplicationId: &stid,
		MessageGroupId:         &c.messageId,
	})
	return err
}

func (c *SqsClient) ReadFromQueue(max int64) error {
	msgResult, err := c.client.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            &c.queueUrl,
		MaxNumberOfMessages: aws.Int64(max),
	})
	if err != nil {
		return err
	}
	log.Info("messages len %s", msgResult.String())
	return nil
}
