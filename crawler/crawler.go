package crawler

import (
	"context"
	"encoding/json"
	"ethparser/graph/model"
	"fmt"
	"os"
	"sync"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Fetcher struct {
	blocks []*model.Block
	mutex  sync.RWMutex
}

func (f *Fetcher) Blocks() []*model.Block {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	return f.blocks
}

func (f *Fetcher) Start() {
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

			b := &model.Block{
				Number:       int(recentBlock.Number().Int64()),
				Transactions: []*model.Transaction{},
			}

			for _, tx := range recentBlock.Transactions() {
				t := &model.Transaction{
					Hash:     tx.Hash().Hex(),
					Nonce:    int(tx.Nonce()),
					Value:    tx.Value().String(),
					GasPrice: int(tx.GasPrice().Int64()),
					Gas:      int(tx.Gas()),
					Type:     int(tx.Type()),
					Data:     string(tx.Data()),
				}
				if tx.To() != nil {
					t.To = tx.To().Hex()
				}
				b.Transactions = append(b.Transactions, t)
			}

			f.blocks = append(f.blocks, b)

			blockJson, err := json.MarshalIndent(b, "", "\t")
			if err != nil {
				fmt.Println("Failed to marshal into JSON format, error", err)
			}
			fmt.Println(string(blockJson))
		}
	}

}
