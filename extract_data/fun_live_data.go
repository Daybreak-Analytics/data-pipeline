package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	gcloud "cloud.google.com/go/storage"
	"github.com/fxamacker/cbor/v2"
	flowgosdk "github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/client"
	"github.com/onflow/flow-go/engine/execution/computation/computer/uploader"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func GetLiveData(date string) {

	ctx := context.Background()

	flow, err := client.New("access.mainnet.nodes.onflow.org:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("Failed to establish connection with the Access API")
	}

	gcloudClient, _ := gcloud.NewClient(ctx, option.WithoutAuthentication())
	bucket := gcloudClient.Bucket(ExecutionStateBucket)

	decOptions := cbor.DecOptions{}
	decoder, err := decOptions.DecMode()
	if err != nil {
		panic(err)
	}

	start := time.Now()
	start_block, end_block := calculateStartAndEndBlockHeight(date, ctx, flow)
	elapsed := time.Since(start)
	fmt.Println("Calculate Block range Time cost:", elapsed)
	if _, err := os.Stat("./liveEvent/"); os.IsNotExist(err) {
		err := os.Mkdir("./liveEvent/", os.ModePerm)
		if err != nil {
			panic(err)
		}
	}

	// start_block, end_block := 36312911, 36374439
	start = time.Now()
	getBlockDataForGivenBlockMultiThread(ctx, flow, uint64(start_block), uint64(end_block), "./liveEvent/"+date+".tsv", bucket, decoder)
	elapsed = time.Since(start)
	fmt.Println("Look for event and write them Time cost:", elapsed)

}

func calculateStartAndEndBlockHeight(targetDate string, ctx context.Context, flowClient *client.Client) (uint64, uint64) {
	latestBlock, _ := flowClient.GetLatestBlock(ctx, true)
	secondsMintBlockNumber := 1
	dateTime, err := time.Parse("2006-01-02", targetDate)
	if err != nil {
		panic(err)
	}
	startBlockHeight := latestBlock.Height - uint64((time.Now().Unix()-dateTime.Unix())*int64(secondsMintBlockNumber))
	endBlockHeight := latestBlock.Height

	startBlockHeight, endBlockHeight = calculatePrecision(dateTime, startBlockHeight, endBlockHeight, ctx, flowClient)
	return startBlockHeight, endBlockHeight

}

// 这里传入的startBlockheight 和 endBlockHeight 一定是 超集
func calculatePrecision(target time.Time, startBlockHeight uint64, endBlockHeight uint64, ctx context.Context, flowClient *client.Client) (uint64, uint64) {
	start, end := startBlockHeight, endBlockHeight
	for {
		startBlock, _ := flowClient.GetBlockByHeight(ctx, start)
		endBlock, _ := flowClient.GetBlockByHeight(ctx, end)
		if target.Unix() < startBlock.Timestamp.Unix() && target.AddDate(0, 0, 1).Unix() > endBlock.Timestamp.Unix() {
			fmt.Println("===================================")
			fmt.Println(startBlock.Height, "--", startBlock.Timestamp)
			fmt.Println(endBlock.Height, "--", endBlock.Timestamp)
			fmt.Println("===================================")
			break
		} else if target.Unix() > startBlock.Timestamp.Unix() && target.AddDate(0, 0, 1).Unix() > endBlock.Timestamp.Unix() {
			start = start + 1

		} else if target.Unix() < startBlock.Timestamp.Unix() && target.AddDate(0, 0, 1).Unix() < endBlock.Timestamp.Unix() {
			end = end - 1
		} else {
			start += 1
			end -= 1
		}
	}
	return start, end
}

//////////////////////////////////////////////////////////////////

func getBlockDataForGivenBlockMultiThread(ctx context.Context, flow *client.Client, start_block_height_processed uint64, end_block_height_processed uint64, filePath string, bucket *gcloud.BucketHandle, decoder cbor.DecMode) {
	i := start_block_height_processed
	batch := BATCH
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
	}
	f.Seek(0, 0)
	defer f.Close()

	data := make(chan string)
	wg := sync.WaitGroup{}
	for {
		if i > end_block_height_processed {
			break
		}

		for j := 0; j < batch; j++ {
			wg.Add(1)
			go getBlockMultiThread(ctx, flow, i+uint64(j), bucket, decoder, data, &wg, f)
		}

		// go writeContentToFile(data, done, f)
		// go func() {
		// 	wg.Wait()
		// 	close(data)
		// }()
		// <-done
		wg.Wait()

		i += uint64(batch)
		if (i-start_block_height_processed)%1000 == 0 {
			fmt.Println("Processed:", i-start_block_height_processed, end_block_height_processed-start_block_height_processed)
		}
	}
}

func getBlockMultiThread(ctx context.Context, flow *client.Client, height uint64, bucket *gcloud.BucketHandle, decoder cbor.DecMode, dataChannel chan string, wg *sync.WaitGroup, f *os.File) {
	contents := ""
	header, _ := flow.GetBlockByHeight(ctx, height)
	object := bucket.Object(fmt.Sprintf("%s.cbor", header.ID.String()))
	reader, err_new_reader := object.NewReader(ctx)
	if err_new_reader != nil {
		fmt.Println("New Reader Wrong BlockID", header.ID.String())
	} else {
		data, err_reader := io.ReadAll(reader)
		if err_reader != nil {
			fmt.Println("Reader All Wrong", header.ID.String(), header.Height, err_reader)
		}
		var record uploader.BlockData
		err_decoder := decoder.Unmarshal(data, &record)
		if err_decoder != nil {
			fmt.Println("Decoder Error BlockID", header.ID.String(), header.Height, err_decoder)
		}
		if len(header.CollectionGuarantees) != 0 && err_decoder == nil && err_reader == nil {
			if len(record.Events) != 0 {
				txid := record.Events[0].TransactionID
				tx, err := flow.GetTransaction(ctx, flowgosdk.Identifier(txid))
				if err != nil {
					fmt.Println("Event0 GetTransactionError-", height, "--", txid, err)
				} else {
					authorizers := ""
					for index, authorizer := range tx.Authorizers {
						if index == len(tx.Authorizers)-1 {
							authorizers += authorizer.String()
						} else {
							authorizers += authorizer.String() + LIVE_SEPARATOR
						}
					}

					for _, event := range record.Events {
						flag := false
						eventType := event.Type
						eventParameter := string(event.Payload)
						if event.TransactionID != txid {
							txid = event.TransactionID
							tx, err = flow.GetTransaction(ctx, flowgosdk.Identifier(txid))
							if err != nil {
								fmt.Println("GetTransactionError-", height, "--", txid, err)
								flag = true
								break
							}
							authorizers = ""
							for index, authorizer := range tx.Authorizers {
								if index == len(tx.Authorizers)-1 {
									authorizers += authorizer.String()
								} else {
									authorizers += authorizer.String() + LIVE_SEPARATOR
								}
							}
						}

						if flag == false {
							content := header.ID.String() + WRITE_SEPARATOR + event.TransactionID.String() + WRITE_SEPARATOR + tx.ProposalKey.Address.String() + WRITE_SEPARATOR + tx.Payer.String() + WRITE_SEPARATOR + authorizers + WRITE_SEPARATOR + strconv.FormatUint(uint64(record.Block.Header.Timestamp.Unix()), 10) + WRITE_SEPARATOR + string(eventType) + WRITE_SEPARATOR + eventParameter
							contents += content
						}
					}
				}

			}

		}
	}

	writeContentToFileMutex(f, contents)
	// dataChannel <- contents
	wg.Done()
}

func writeContentToFile(dataChannel chan string, done chan bool, f *os.File) {
	for d := range dataChannel {
		_, err := f.WriteString(d)
		if err != nil {
			fmt.Println(err)
			done <- false
			return
		}
	}
	done <- true
}

func writeContentToFileMutex(f *os.File, content string) {
	Lock.Lock()
	defer Lock.Unlock()
	f.WriteString(content)
}
