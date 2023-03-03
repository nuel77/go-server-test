package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoDBClient struct {
	*dynamodb.DynamoDB
	TableName string
}

func NewDynamoDBClient(sess *session.Session, tableName string) *DynamoDBClient {
	return &DynamoDBClient{
		DynamoDB:  dynamodb.New(sess),
		TableName: tableName,
	}
}

func (c *DynamoDBClient) ReadItem(HashKey string, RangeKey string) (*dynamodb.GetItemOutput, error) {
	result, err := c.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(c.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"hash_key": {
				N: aws.String(HashKey),
			},
			"range_key": {
				S: aws.String(RangeKey),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return result, err
}

func (c *DynamoDBClient) WriteItem(HashKey string, Range string, data string) error {
	_, err := c.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(c.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			"hash_key": {
				N: aws.String(HashKey),
			},
			"range_key": {
				S: aws.String(Range),
			},
			"data": {
				S: aws.String(data),
			},
		},
	})
	return err
}
