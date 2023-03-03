package main

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net/http"
	"os"
)

var (
	AwsRegion    string
	SqsQueueName string
	SnsTopicArn  string
	S3Bucket     string
)
var sess *session.Session
var sqsClient *SqsClient
var snsClient *SnsClient
var log *zap.SugaredLogger
var S3Uploader *s3manager.Uploader
var S3Downloader *s3manager.Downloader

func main() {
	//load env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	//init logger
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, _ := config.Build()
	log = logger.Sugar()

	//init env vars
	AwsRegion = os.Getenv("AWS_REGION")
	SqsQueueName = os.Getenv("SQS_QUEUE_NAME")
	SnsTopicArn = os.Getenv("SNS_TOPIC_ARN")
	S3Bucket = os.Getenv("S3_BUCKET")

	//init aws clients
	sess = session.Must(session.NewSession(&aws.Config{Region: aws.String(AwsRegion)}))
	sqsClient = NewSqsClient(sess, SqsQueueName)
	snsClient = NewSnsClient(sess)
	S3Uploader = s3manager.NewUploader(sess)
	S3Downloader = s3manager.NewDownloader(sess)

	//init http server
	http.HandleFunc("/s3UploadSnapshot", HandleSnapShotUpload)
	http.HandleFunc("/s3DownloadSnapshot", HandleSnapShotDownload)
	http.HandleFunc("getStateChange", HandleGetStateChange)
	http.HandleFunc("/getSQSMessage", HandleSqsRead)
	http.HandleFunc("/putSnsMessage", HandleSnsPush)
	http.HandleFunc("getEnclaveInitData", HandleGetEnclaveInitData)
	http.HandleFunc("putEnclaveInitData", HandlePutEnclaveInitData)

	log.Info("server starting at port:3333")

	//start http server
	err = http.ListenAndServe(":3333", nil)
	if errors.Is(err, http.ErrServerClosed) {
		log.Error("server is closed")
	} else if err != nil {
		log.Errorf("error starting server : %s", err.Error())
	}
}
