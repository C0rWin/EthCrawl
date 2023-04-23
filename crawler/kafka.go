package crawler

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

// KafkaManager is a struct that holds the Kafka producer
type KafkaManager struct {
	Producer sarama.SyncProducer
	Config   *sarama.Config
	kafkaURI string
}

// NewKafkaManager creates a new KafkaManager
func NewKafkaManager(kafkaURI string) (*KafkaManager, error) {
	producer, err := sarama.NewSyncProducer(strings.Split(kafkaURI, ","), nil)
	if err != nil {
		return nil, err
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second

	return &KafkaManager{
		Producer: producer,
		Config:   config,
		kafkaURI: kafkaURI,
	}, nil
}

// Close closes the Kafka producer
func (f *KafkaManager) Close() {
	f.Producer.Close()
}

// CreateTopic creates a new topic in Kafka
func (f *KafkaManager) CreateTopic(topic string) error {
	admin, err := sarama.NewClusterAdmin(strings.Split(f.kafkaURI, ","), nil)
	if err != nil {
		return err
	}
	defer admin.Close()
	return admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
}

// SyncProducer is a Kafka producer
func (f *KafkaManager) SyncProducer(kafkaURI string) (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer(strings.Split(kafkaURI, ","), nil)
}
