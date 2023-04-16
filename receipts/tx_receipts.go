package main

// import logrus "github.com/sirupsen/logrus"

import (
	"context"
	"ethparser/graph/model"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {

	// setup logrus
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)

	// setup logger name
	log.WithFields(log.Fields{
		"logger": "tx_receipts",
	})

	// Need to get parameters:
	//
	// 1. API ID to query Ethereum Client
	// 2. MongoDB endpoint
	if len(os.Args) != 3 {
		log.Error("Wrong amount of parameters")
		os.Exit(-1)
	}

	mongoURI := fmt.Sprintf("mongodb://%s", os.Args[1])
	ethClientURI := fmt.Sprintf("wss://mainnet.infura.io/ws/v3/%s", os.Args[2])

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		interruptCh := make(chan os.Signal, 1)
		signal.Notify(interruptCh, os.Interrupt, syscall.SIGTERM)
		<-interruptCh
		cancel()
	}()

	// Need to read stream of block from mongo db, and
	// interate over transactions to query for receipts
	// by using ethereum client and enreach data within
	// mongo database. There is a need to keep correlation
	// between the block id and the transaction.

	mongoClient, err := mongo.NewClient(options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Error("Error connecting to mongodb, error = ", err)
		os.Exit(-1)
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = mongoClient.Connect(ctxTimeout)
	if err != nil {
		log.Error("Error connecting to mongodb, error = ", err)
		os.Exit(-1)
	}

	// check connection with the leader
	err = mongoClient.Ping(ctxTimeout, readpref.Primary())
	if err != nil {
		log.Error("Error sending ping to the mongo db server, error", err)
		os.Exit(-1)
	}

	blocks := mongoClient.Database("BlocksDB").Collection("blocks")

	blocksCursor, err := blocks.Find(ctx, bson.M{})
	if err != nil {
		log.Error("Failed to create cursor to listen for blocks, error", err)
		os.Exit(-1)
	}
	defer blocksCursor.Close(ctx)

	receipts := mongoClient.Database("BlocksDB").Collection("receipts")

	clientEth, err := ethclient.Dial(ethClientURI)
	if err != nil {
		panic(err)
	}

	ch := make(chan *model.Block)
	defer close(ch)

	// start goroutine to read blocks from the stream
	go func() {
		for blocksCursor.Next(ctx) {
			block := &model.Block{}
			if err := blocksCursor.Decode(block); err != nil {
				log.Error("Failed to decode block data from stream, error", err)
				os.Exit(-1)
			}
			ch <- block
		}
	}()

	// reading transactions from the channel and exit if context
	// was cancelled
	for {
		select {
		case <-ctx.Done():
			log.Info("Context is done, exiting")
			return
		case block := <-ch:
			for _, tx := range block.Transactions {
				// use tx hash value to retreive transaction receipt from
				// the network
				receipt, err := clientEth.TransactionReceipt(ctx, common.HexToHash(tx.Hash))
				if err != nil {
					log.Error("Failed to receive transaction receipt, err", err)
					os.Exit(-1)
				}

				receipt.BlockNumber = big.NewInt(int64(block.Number))
				_, err = receipts.InsertOne(ctx, receipt)
				if err != nil {
					log.Error("Failed to insert receipt into database, err", err)
					os.Exit(-1)
				}
			}
		}
	}
}
