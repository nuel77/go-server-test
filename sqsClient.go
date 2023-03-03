package main

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SqsClient struct {
	client            *sqs.SQS
	queueName         string
	queueUrl          string
	messageId         string
	lastReceiptHandle string
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

func (c *SqsClient) ReadFromQueue(max int64) ([]byte, error) {
	//send ack for last message
	if c.lastReceiptHandle != "" {
		_, err := c.client.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      &c.queueUrl,
			ReceiptHandle: &c.lastReceiptHandle,
		})
		if err != nil {
			return nil, err
		}
	}
	//read new messages
	msgResult, err := c.client.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            &c.queueUrl,
		MaxNumberOfMessages: aws.Int64(max),
	})
	if err != nil {
		return nil, err
	}
	result, err := json.Marshal(msgResult.Messages)
	if err != nil {
		return nil, err
	}
	//save the receipt handle for the last message
	length := len(msgResult.Messages)
	if length == 0 {
		return nil, nil
	}
	c.lastReceiptHandle = *msgResult.Messages[length-1].ReceiptHandle
	return result, nil
}
