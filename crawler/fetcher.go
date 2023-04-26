package crawler

import (
	"context"
	"encoding/json"
	"ethparser/graph/model"
	logger "ethparser/log"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/sirupsen/logrus"
)

const (
	// KafkaInBlocksTopic is the topic name for blocks
	KafkaInBlocksTopic = "InBlocks"
)

var (
	// define package wide logger
	log = logger.NewLogger("crawler", logrus.DebugLevel)
)

// Fetcher is a struct that fetches blocks from the Ethereum network
// and sends to the Kafka topic
type Fetcher struct {
	Producer sarama.SyncProducer
	Client   *ethclient.Client
}

// Start starts the fetcher
func (f *Fetcher) Start(ctx context.Context) {
	recentBlockNum, err := f.Client.BlockNumber(ctx)
	if err != nil {
		panic(err)
	}

	blocksDB, err := leveldb.New("ledger/blocks", 0, 0, "ethereum", false)
	if err != nil {
		panic(err)
	}

	retreiveTicker := time.NewTicker(time.Millisecond)
	defer retreiveTicker.Stop()

	nextBlockNum := big.NewInt(0)
	lastBlock, err := blocksDB.Get([]byte("lastBlock"))
	if err == nil {
		err = json.Unmarshal(lastBlock, nextBlockNum)
		if err != nil {
			panic(fmt.Sprintf("cannot Unmarshal last block number from the DB, %s", err))
		}
	}

	for nextBlockNum != big.NewInt(int64(recentBlockNum)) {
		select {
		case <-ctx.Done():
			log.Info("Stop fetching blocks")
			return
		case <-retreiveTicker.C:
			nextBlockNum := nextBlockNum.Add(nextBlockNum, big.NewInt(1))
			n := nextBlockNum.Uint64()
			log.Debugf("Retreiving block number %d\n", n)
			recentBlock, err := f.Client.BlockByNumber(ctx, nextBlockNum)
			if err != nil {
				fmt.Printf("ALERT: received an error, %s\n", err)
				continue
			}

			blockBytes, err := rlp.EncodeToBytes(recentBlock)
			if err != nil {
				log.Error("Failed to encode block due to, error", err)
				os.Exit(-1)
			}
			blocksDB.Put(recentBlock.Hash().Bytes(), blockBytes)
			recentBlockJSON, err := json.Marshal(recentBlock.Number())
			if err != nil {
				panic(fmt.Sprintf("failed to marhsal recent block numbers, %s", err))
			}
			blocksDB.Put([]byte("lastBlock"), recentBlockJSON)

			b := f.DumpBlock(ctx, recentBlock)
			bJSON, err := json.Marshal(b)
			if err != nil {
				panic(fmt.Sprintf("Failed to marshal block to JSON, error %s", err))
			}
			_, _, err = f.Producer.SendMessage(&sarama.ProducerMessage{
				Topic: KafkaInBlocksTopic,
				Value: sarama.StringEncoder(string(bJSON)),
			})
			log.Debug("Sent block to Kafka topic", string(bJSON))
			if err != nil {
				log.Debug("Failed to post block json content to Kafka topic, error", err)
			}
		}
	}
}

// DumpBlock creates a new block
func (f *Fetcher) DumpBlock(ctx context.Context, block *types.Block) *model.Block {
	b := &model.Block{
		Number:       int(block.Number().Int64()),
		Transactions: []*model.Transaction{},
	}

	log.Debugf("Block number %d has %d transactions\n", block.Number().Int64(), len(block.Transactions()))
	for idx, tx := range block.Transactions() {
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
		fromAddr, err := f.Client.TransactionSender(ctx, tx, block.Hash(), uint(idx))
		if err == nil {
			t.From = fromAddr.Hex()
		}

		// retreive transaction receipt
		receipt, err := f.Client.TransactionReceipt(ctx, tx.Hash())
		if err != nil {
			panic(fmt.Sprintf("ALERT: failed to receive transaction receipt, err %s", err))
		}

		// copy receipt to the transaction
		t.Receipt = &model.Receipt{
			Type:              int(receipt.Type),
			CumulativeGasUsed: int(receipt.CumulativeGasUsed),
			Logs:              []*model.Log{},
			TxHash:            receipt.TxHash.Hex(),
			ContractAddress:   receipt.ContractAddress.Hex(),
			GasUsed:           int(receipt.GasUsed),
			BlockHash:         receipt.BlockHash.Hex(),
			BlockNumber:       int(receipt.BlockNumber.Int64()),
			TransactionIndex:  int(receipt.TransactionIndex),
		}
		// add all log events
		for _, log := range receipt.Logs {
			l := &model.Log{
				Address:          log.Address.Hex(),
				Topics:           []string{},
				Data:             string(log.Data),
				BlockHash:        log.BlockHash.Hex(),
				BlockNumber:      int(log.BlockNumber),
				TransactionHash:  log.TxHash.Hex(),
				TransactionIndex: int(log.TxIndex),
				LogIndex:         int(log.Index),
				Removed:          log.Removed,
			}
			// copy topics
			for _, topic := range log.Topics {
				l.Topics = append(l.Topics, topic.Hex())
			}
			t.Receipt.Logs = append(t.Receipt.Logs, l)
		}

		b.Transactions = append(b.Transactions, t)
	}

	return b
}
