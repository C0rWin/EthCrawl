package main

import (
	"context"
	"encoding/json"
	"ethparser/graph/model"
	logger "ethparser/log"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
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
	NetworkURI string
	Producer   sarama.SyncProducer
}

// Start starts the fetcher
func (f *Fetcher) Start(ctx context.Context) {
	client, err := ethclient.Dial(f.NetworkURI)
	if err != nil {
		panic(err)
	}

	recentBlockNum, err := client.BlockNumber(ctx)
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
			fmt.Println("Stop fetching blocks")
			return
		case <-retreiveTicker.C:
			nextBlockNum := nextBlockNum.Add(nextBlockNum, big.NewInt(1))
			n := nextBlockNum.Uint64()
			fmt.Printf("Retreiving block number %d\n", n)
			recentBlock, err := client.BlockByNumber(ctx, nextBlockNum)
			if err != nil {
				fmt.Printf("ALERT: received an error, %s\n", err)
				continue
			}

			blockBytes, err := rlp.EncodeToBytes(recentBlock)
			if err != nil {
				fmt.Println("Failed to encode block due to, error", err)
				os.Exit(-1)
			}
			blocksDB.Put(recentBlock.Hash().Bytes(), blockBytes)
			recentBlockJSON, err := json.Marshal(recentBlock.Number())
			if err != nil {
				panic(fmt.Sprintf("failed to marhsal recent block numbers, %s", err))
			}
			blocksDB.Put([]byte("lastBlock"), recentBlockJSON)

			b := &model.Block{
				Number:       int(recentBlock.Number().Int64()),
				Transactions: []*model.Transaction{},
			}

			fmt.Printf("Block number %d has %d transactions\n", recentBlock.Number().Int64(), len(recentBlock.Transactions()))
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

				// retreive transaction receipt
				receipt, err := client.TransactionReceipt(ctx, tx.Hash())
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
			bJSON, err := json.Marshal(b)
			if err != nil {
				panic(fmt.Sprintf("Failed to marshal block to JSON, error %s", err))
			}
			_, _, err = f.Producer.SendMessage(&sarama.ProducerMessage{
				Topic: KafkaInBlocksTopic,
				Value: sarama.StringEncoder(string(bJSON)),
			})
			fmt.Println("Sent block to Kafka topic", string(bJSON))
			if err != nil {
				fmt.Println("Failed to post block json content to Kafka topic, error", err)
			}
		}
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Wrong parameters")
		os.Exit(-1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		interruptCh := make(chan os.Signal, 1)
		signal.Notify(interruptCh, os.Interrupt, syscall.SIGTERM)
		<-interruptCh
		cancel()
	}()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second

	kafkaURI := os.Args[2]
	client, err := sarama.NewClient([]string{kafkaURI}, config)
	if err != nil {
		panic(fmt.Sprintln("Failed creating Kafka client, error", err))
	}
	defer client.Close()

	broker := client.Brokers()[0]
	err = broker.Open(nil)
	if err != nil {
		fmt.Println("Falied to open connection to the broker, error", err)
		os.Exit(-1)
	}
	connected, err := broker.Connected()
	if err != nil {
		panic(fmt.Sprintf("Failure to check broker connectivity, erorr %s", err))
	}
	if !connected {
		fmt.Println("Kafka broker is not connected")
		os.Exit(-1)
	}

	_, err = broker.CreateTopics(&sarama.CreateTopicsRequest{
		Timeout: 15 * time.Second,
		TopicDetails: map[string]*sarama.TopicDetail{
			KafkaInBlocksTopic: {
				NumPartitions:     int32(1),
				ReplicationFactor: int16(1),
				ConfigEntries:     map[string]*string{},
			},
		},
	})
	if err != nil {
		fmt.Println("Failed to create Kafka topic, error", err, "exiting...")
		os.Exit(-1)
	}

	producer, err := sarama.NewSyncProducer([]string{kafkaURI}, config)
	if err != nil {
		fmt.Println("Failed to get connected with Kafka, error = ", err)
		os.Exit(-1)
	}
	defer producer.Close()

	fetcher := &Fetcher{
		NetworkURI: fmt.Sprintf("wss://mainnet.infura.io/ws/v3/%s", os.Args[1]),
		Producer:   producer,
	}

	go fetcher.Start(ctx)

	select {
	case <-ctx.Done():
		fmt.Println("Exiting")
	}
}
