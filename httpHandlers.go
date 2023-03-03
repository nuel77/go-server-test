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

func HandlePutEnclaveInitData(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Errorf("error reading request body %s", err)
		return
	}
	_, err = S3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(S3Bucket),
		Key:    aws.String("enclave_init_data.bin"),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error uploading file %s", err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func HandleGetEnclaveInitData(w http.ResponseWriter, r *http.Request) {
	//get enclave init data from dynamodb
	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := S3Downloader.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(S3Bucket),
		Key:    aws.String("enclave_init_data.bin"),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error downloading file %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(buf.Bytes())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error writing to response %s", err.Error())
		return
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

func HandleGetStateChange(w http.ResponseWriter, r *http.Request) {
	//read the header value to get state_id
	stateId := r.Header.Get("STATE_ID")
	if stateId == "" {
		http.Error(w, "state id not found", http.StatusBadRequest)
		return
	}
	log.Infof("received get state change from enclave: %s", stateId)
	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := S3Downloader.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(S3Bucket),
		Key:    aws.String(stateId),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error downloading file %s", err.Error())
		return
	}
	_, err = w.Write(buf.Bytes())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Errorf("error writing to response %s", err.Error())
		return
	}
}
