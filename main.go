package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net/http"
)

var (
	AwsRegion string
	S3Bucket  string
)
var sess *session.Session
var log *zap.SugaredLogger
var S3Uploader *s3manager.Uploader
var S3Downloader *s3manager.Downloader

func main() {
	//init logger
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, _ := config.Build()
	log = logger.Sugar()

	//init global vars
	AwsRegionFlag := flag.String("awsregion", "ap-south-1", "region of aws")
	S3BucketFlag := flag.String("s3bucket", "", "s3 bucket name of aws")
	portFlag := flag.Int("port", 3333, "port to run server on")
	flag.Parse()

	AwsRegion = *AwsRegionFlag
	S3Bucket = *S3BucketFlag
	port := *portFlag

	if len(S3Bucket) == 0 {
		log.Fatal("s3 bucket name is empty")
	}
	log.Info("s3 bucket name: ", S3Bucket)
	log.Infof("aws region: %s", AwsRegion)
	log.Infof("port: %d", port)
	//init aws clients
	sess = session.Must(session.NewSession(&aws.Config{Region: aws.String(AwsRegion)}))
	S3Uploader = s3manager.NewUploader(sess)
	S3Downloader = s3manager.NewDownloader(sess)

	//init http server
	http.HandleFunc("/ping", HandlePing)
	http.HandleFunc("/s3UploadSnapshot", HandleSnapShotUpload)
	http.HandleFunc("/s3DownloadSnapshot", HandleSnapShotDownload)
	log.Infof("server starting at port %d", port)

	//start http server
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if errors.Is(err, http.ErrServerClosed) {
		log.Error("server is closed")
	} else if err != nil {
		log.Errorf("error starting server : %s", err.Error())
	}
}
