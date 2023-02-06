package crawler

import (
	"context"
	"ethparser/graph/model"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/asm"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Fetcher struct {
	blocks     []*model.Block
	mutex      sync.RWMutex
	NetworkURI string
}

func (f *Fetcher) Blocks() []*model.Block {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	return f.blocks
}

func (f *Fetcher) Start(ctx context.Context) {
	client, err := ethclient.Dial(f.NetworkURI)
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
		case <-ctx.Done():
			fmt.Println("Stop fetching blocks")
			return
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

			for idx, tx := range recentBlock.Transactions() {
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
				fromAddr, err := client.TransactionSender(ctx, tx, recentBlock.Hash(), uint(idx))
				if err == nil {
					t.From = fromAddr.Hex()
				}
				b.Transactions = append(b.Transactions, t)

				receipt, err := client.TransactionReceipt(context.Background(), tx.Hash())
				if err != nil {
					fmt.Println("ALERT: failed to receive transaction receipt, err", err)
					continue
				}
				if receipt == nil || receipt.ContractAddress == (common.Address{}) {
					continue
				}

				contractAddress := receipt.ContractAddress
				code, err := client.CodeAt(context.Background(), contractAddress, nil)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Println("Bytecode:", code)

				contractText, err := asm.Disassemble(code)
				if err != nil {
					fmt.Println("ALERT: Failed to decompile contract code, error", err)
					continue
				}
				fmt.Println("Smart contract:", contractText)
			}

			f.blocks = append(f.blocks, b)
		}
	}

}
