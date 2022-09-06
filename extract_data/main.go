package main

import (
	"os"
	"sync"
)

var number int = 50
var ch = make(chan int, number)
var chBranch = make(chan int, number)
var WRITE_SEPARATOR = "\t"
var LIVE_SEPARATOR = ","
var BATCH = 25
var ExecutionStateBucket = "flow_public_mainnet19_execution_state"
var Lock = sync.Mutex{}

func main() {
	// Mainnet1-3, 14
	// extractContentsFromBadgetProtocol("./db/mainnet1/protocol/", "./newevent/mainnet1.tsv")

	// UpdateQueryBlockDate()
	// Mainnet17
	GetLiveData(os.Args[1])

}
