package main

import (
	"bytes"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"net/http"
	"os"
	"strconv"
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
	http.HandleFunc("/s3Upload", HandleSnapShotUpload)
	http.HandleFunc("/s3Download", HandleSnapShotDownload)
	http.HandleFunc("/readFromSqs", HandleSqsRead)
	http.HandleFunc("/putToSns", HandleSnsPush)

	log.Info("server starting at port:3333")

	//start http server
	err = http.ListenAndServe(":3333", nil)
	if errors.Is(err, http.ErrServerClosed) {
		log.Error("server is closed")
	} else if err != nil {
		log.Errorf("error starting server : %s", err.Error())
	}
}

func HandleSnsPush(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = snsClient.PublishToSns(string(data))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error publishing to sns %s", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func HandleSqsRead(w http.ResponseWriter, r *http.Request) {
	ReceiptHandle, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Debug("receipt handle: ", string(ReceiptHandle))
	//delete last message if receipt handle exists
	if len(ReceiptHandle) != 0 {
		err = sqsClient.DeleteLastMessage(string(ReceiptHandle))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Errorf("error deleting last message %s", err.Error())
			return
		}
	}
	res, err := sqsClient.ReadFromQueue(1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Infof("error reading from queue %s", err.Error())
		return
	}
	_, err = w.Write(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error writing to response %s", err.Error())
		return
	}
}

func HandleSnapShotDownload(w http.ResponseWriter, r *http.Request) {
	// Create a buffer to WriteAt to.
	buf := aws.NewWriteAtBuffer([]byte{})
	// Write the contents of S3 Object to the file
	_, err := S3Downloader.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(S3Bucket),
		Key:    aws.String("file_test"),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error downloading file %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(buf.Bytes())))
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(buf.Bytes())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error writing to response %s", err.Error())
	}
}

func HandleSnapShotUpload(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Info("uploading snapshot from enclave to s3")
	// Upload the file to S3.
	_, err = S3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(S3Bucket),
		Key:    aws.String("file_test2"),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error uploading file %s", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}
