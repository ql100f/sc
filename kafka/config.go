package kafka

import (
	"time"

	"github.com/IBM/sarama"
)

const (
	topicName = "test-topic"
)

type KafkaConfig struct {
	BrokerAddr []string
	Version    sarama.KafkaVersion
}

var GlobalKafkaConfig = &KafkaConfig{
	BrokerAddr: []string{"localhost:9094"},
	Version:    sarama.V4_3_1_0,
}

func NewSyncProducer() (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = GlobalKafkaConfig.Version
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	return sarama.NewSyncProducer(GlobalKafkaConfig.BrokerAddr, cfg)
}

func NewConsumerGroup(groupID string) (sarama.ConsumerGroup, error) {
	cfg := sarama.NewConfig()
	cfg.Version = GlobalKafkaConfig.Version
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = time.Second
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return sarama.NewConsumerGroup(GlobalKafkaConfig.BrokerAddr, groupID, cfg)
}
