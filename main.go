package main

import (
	"context"
	"encoding/json"
	"ethparser/custom"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {
	network := fmt.Sprintf("wss://mainnet.infura.io/ws/v3/%s", os.Args[1])
	client, err := ethclient.Dial(network)
	if err != nil {
		panic(err)
	}

	headers := make(chan *types.Header)
	sub, err := client.SubscribeNewHead(context.Background(), headers)
	if err != nil {
		panic(err)
	}

	for {
		select {
		case err := <-sub.Err():
			panic(err)
		case h := <-headers:
			recentBlock, err := client.BlockByNumber(context.TODO(), h.Number)
			if err != nil {
				fmt.Printf("ALERT: received an error, %s\n", err)
				continue
			}

			b, err := custom.FromEthBlock(recentBlock)
			if err != nil {
				fmt.Println("Failed to read and parse next block, error", err)
				continue
			}

			blockJson, err := json.MarshalIndent(b, "", "\t")
			if err != nil {
				fmt.Println("Failed to marshal into JSON format, error", err)
			}
			fmt.Println(string(blockJson))
		}
	}
}
