package main

import (
	"context"
	"fmt"

	gcloud "cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func spork_twenty() {
	ctx := context.Background()

	// flow, err := client.New("access.mainnet.nodes.onflow.org:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	// if err != nil {
	// 	log.Fatal("Failed to establish connection with the Access API")
	// }

	gcloudClient, _ := gcloud.NewClient(ctx, option.WithoutAuthentication())
	bucket := gcloudClient.Bucket("flow_public_mainnet20_execution_state")

	block1 := "9898522933e9395f687462e921de4df6e594cf02f7744ca34334cfc9544d4eef"
	block2 := "46b9428a0515169e869bce78e25904c4092467e46d8d0bbbd7472f31e5d5c8c1"
	object := bucket.Object(fmt.Sprintf("%s.cbor", block1))
	_, err_new_reader := object.NewReader(context.Background())
	if err_new_reader != nil {
		fmt.Println(block1, "Error", err_new_reader)
	} else {
		fmt.Println(block1, "Nice")
	}

	object = bucket.Object(fmt.Sprintf("%s.cbor", block2))
	_, err_new_reader = object.NewReader(context.Background())
	if err_new_reader != nil {
		fmt.Println(block2, "Error", err_new_reader)
	} else {
		fmt.Println(block2, "Nice")
	}

	// header, _ := flow.GetBlockByHeight(ctx, 40171734)
	// object := bucket.Object("120aea8dafe15a084d2d0ce12e46d29168742d1d5c75009cb10db644b8e5cac5.cbor")
	// _, err_new_reader := object.NewReader(context.Background())
	// if err_new_reader != nil {
	// 	fmt.Println("New Reader Wrong BlockID", header.ID.String(), err_new_reader, 40171734, fmt.Sprintf("%s.cbor", header.ID.String()))
	// 	fmt.Println(err_new_reader)
	// } else {
	// 	fmt.Println("Nice")
	// }

}
