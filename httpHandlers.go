package main

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"net/http"
	"strconv"
)

func HandlePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("pong"))
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
		log.Errorf("error reading body %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	//get file name for header
	filename := r.Header.Get("FILE_NAME")
	log.Info("file name: ", filename)
	if filename == "" {
		log.Errorf("file name is empty")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Info("uploading snapshot from enclave to s3")
	// Upload the file to S3.
	_, err = S3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(S3Bucket),
		Key:    aws.String(filename),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error uploading file %s", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}
