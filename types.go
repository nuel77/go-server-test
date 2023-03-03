package main

type SqsResponse struct {
	ReceiptHandle string `json:"receipt"`
	Stid          string `json:"stid"`
}
