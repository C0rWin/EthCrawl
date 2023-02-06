package main

import (
	"context"
	"encoding/json"
	"ethparser/graph/model"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/asm"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	KafkaInBlocksTopic = "InBlocks"
)

type Fetcher struct {
	NetworkURI string
	Producer   sarama.SyncProducer
}

func (f *Fetcher) Start(ctx context.Context) {
	client, err := ethclient.Dial(f.NetworkURI)
	if err != nil {
		panic(err)
	}

	headers := make(chan *types.Header)
	sub, err := client.SubscribeNewHead(ctx, headers)
	if err != nil {
		panic(err)
	}

	blocksDB, err := leveldb.New("ledger/blocks", 0, 0, "ethereum", false)
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
			recentBlock, err := client.BlockByNumber(ctx, h.Number)
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

				receipt, err := client.TransactionReceipt(ctx, tx.Hash())
				if err != nil {
					fmt.Println("ALERT: failed to receive transaction receipt, err", err)
					continue
				}
				if receipt == nil || receipt.ContractAddress == (common.Address{}) {
					continue
				}

				contractAddress := receipt.ContractAddress
				code, err := client.CodeAt(ctx, contractAddress, nil)
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
			bJson, err := json.Marshal(b)
			if err != nil {
				panic(fmt.Sprintf("Failed to marshal block to JSON, error %s", err))
			}
			_, _, err = f.Producer.SendMessage(&sarama.ProducerMessage{
				Topic: KafkaInBlocksTopic,
				Value: sarama.StringEncoder(string(bJson)),
			})
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
