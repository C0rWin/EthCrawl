package main

import (
	"context"
	"encoding/json"
	"ethparser/graph/model"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BlocksConsumer reads blocks from the Kafka topic
// and injects into storage for persistance
type BlocksConsumer struct {
	ParitionConsumer sarama.PartitionConsumer
	Store            *BlocksStore
}

func (b *BlocksConsumer) Consume(ctx context.Context) {
	for {
		select {
		case msg := <-b.ParitionConsumer.Messages():
			block := &model.Block{}
			json.Unmarshal(msg.Value, block)
			if err := b.Store.Persist(ctx, block); err != nil {
				fmt.Println("Failed to store block, error", err)
				continue
			}

		case <-ctx.Done():
			return
		}
	}
}

// BlocksStore persists blocks into storage
type BlocksStore struct {
	BlocksDB *mongo.Database
}

func (bs *BlocksStore) Persist(ctx context.Context, block *model.Block) error {
	_, err := bs.BlocksDB.Collection("blocks").InsertOne(ctx, block)
	return err
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Please provide parameters for Kafka and MongoDB URIs")
		os.Exit(-1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		interruptCh := make(chan os.Signal, 1)
		signal.Notify(interruptCh, os.Interrupt, syscall.SIGTERM)
		<-interruptCh
		cancel()
	}()

	client, err := mongo.NewClient(options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", os.Args[1])))
	if err != nil {
		fmt.Println("Failed to create MongoDB instance, erorr", err)
		os.Exit(-1)
	}

	err = client.Connect(ctx)
	if err != nil {
		fmt.Println("Failed to get connected to the MongoDB instance, error", err)
		os.Exit(-1)
	}

	defer client.Disconnect(ctx)

	blockStore := &BlocksStore{
		BlocksDB: client.Database("BlocksDB"),
	}

	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{os.Args[2]}, config)
	if err != nil {
		fmt.Println("Cannot create Kafka Consumer, error", err)
		os.Exit(-1)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("InBlocks", 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Println("Failed to start partition consumer, error", err)
		os.Exit(-1)
	}

	blocksConsumer := &BlocksConsumer{
		ParitionConsumer: partitionConsumer,
		Store:            blockStore,
	}

	go blocksConsumer.Consume(ctx)
	select {
	case <-ctx.Done():
		return
	}
}
