package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"io"
	"net/http"
)

var sess = session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))

func putToSns(w http.ResponseWriter, r *http.Request) {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("ap-south-1")}))
	data, err := io.ReadAll(r.Body)
	topic := "arn:aws:sns:ap-south-1:876703040586:orderbook-service"
	msg := string(data)
	svc := sns.New(sess)
	pubResult, err := svc.Publish(&sns.PublishInput{
		Message:  &msg,
		TopicArn: &topic,
	})
	fmt.Printf("publish result %s", pubResult.String())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Println(err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func readFromSqs(w http.ResponseWriter, r *http.Request) {
	_, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	svc := sqs.New(sess)
	urlResult, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String("nuelqueue.fifo"),
	})
	fmt.Printf("url %s", *urlResult.QueueUrl)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Printf("error getting queue url %s", err)
		return
	}
	msgResult, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            urlResult.QueueUrl,
		MaxNumberOfMessages: aws.Int64(10),
	})
	//messages := msgResult.GoString()
	fmt.Printf("messages len %s", msgResult.String())
	w.WriteHeader(http.StatusOK)
}

func S3Downloader(w http.ResponseWriter, r *http.Request) {
	downloader := s3manager.NewDownloader(sess)
	// Create a buffer to WriteAt to.
	buf := aws.NewWriteAtBuffer([]byte{})
	// Write the contents of S3 Object to the file
	_, err := downloader.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String("nueltest"),
		Key:    aws.String("file_test"),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Printf("error downloading file %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", string(rune(len(buf.Bytes()))))
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(buf.Bytes())
	if err != nil {
		fmt.Printf("error writing to response %s", err)
	}
}

func S3Uploader(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)

	//create a vector of random bytes to upload to s3 of size 1GB
	randData := make([]byte, 1024*1024*1024)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	fmt.Printf("hello from server %s \n", string(data))
	uploader := s3manager.NewUploader(sess)
	// Upload the file to S3.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String("nueltest"),
		Key:    aws.String("file_test2"),
		Body:   bytes.NewReader(randData),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Printf("error uploading file %s", err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func main() {
	http.HandleFunc("/s3Upload", S3Uploader)
	http.HandleFunc("/s3Download", S3Downloader)
	http.HandleFunc("/readFromSqs", readFromSqs)
	http.HandleFunc("/putToSns", putToSns)
	fmt.Println("server listening...")

	err := http.ListenAndServe(":3333", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server is closed\n")
	} else if err != nil {
		fmt.Printf("error starting server : %s\n", err)
	}
}
