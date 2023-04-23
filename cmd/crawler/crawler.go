package main

import (
	"context"
	"ethparser/crawler"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

func main() {
	var kafkaURI string
	var infuraAPIKey string

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		interruptCh := make(chan os.Signal, 1)
		signal.Notify(interruptCh, os.Interrupt, syscall.SIGTERM)
		<-interruptCh
		cancel()
	}()

	// read parameters with cobra, need to read kafka URI and Infura API key
	rootCmd := &cobra.Command{
		Use:   "crawler",
		Short: "Crawler for Ethereum blockchain",
		Long:  "Crawler for Ethereum blockchain",
		Run: func(cmd *cobra.Command, args []string) {
			// create Kafka manager
			manager, err := crawler.NewKafkaManager(kafkaURI)
			if err != nil {
				panic(fmt.Sprintf("Failed to create Kafka manager, error %s", err))
			}
			defer manager.Close()

			// create Kafka topic
			err = manager.CreateTopic(crawler.KafkaInBlocksTopic)
			if err != nil {
				panic(fmt.Sprintf("Failed to create Kafka topic, error %s", err))
			}

			// create Kafka producer
			producer, err := manager.SyncProducer(kafkaURI)
			if err != nil {
				panic(fmt.Sprintf("Failed to create Kafka producer, error %s", err))
			}
			defer producer.Close()

			fetcher := &crawler.Fetcher{
				NetworkURI: fmt.Sprintf("wss://mainnet.infura.io/ws/v3/%s", infuraAPIKey),
				Producer:   producer,
			}

			go fetcher.Start(cmd.Context())

			select {
			case <-ctx.Done():
				fmt.Println("Exiting")
			}
		},
	}

	rootCmd.Flags().StringVar(&kafkaURI, "kafka", "k", "Kafka URI")
	rootCmd.Flags().StringVar(&infuraAPIKey, "infura", "i", "Infura API key")

	if len(os.Args) != 5 {
		fmt.Println("Wrong number of parameters", len(os.Args))
		rootCmd.Help()
		os.Exit(-1)
	}

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
