package kafka

import (
	"context"
	"fmt"
	"sc/internal"

	"github.com/IBM/sarama"
)

func init() {
	internal.Register(&topic{})
}

type topic struct{}

func (t *topic) Name() string {
	return "kafka-topic"
}
func (t *topic) Run(ctx context.Context) error {
	patitionNum := int32(3)
	replicationNum := int16(1)
	err := createTopic(nil, "topic-order", patitionNum, replicationNum)
	if err != nil {
		return fmt.Errorf("create topic err : %w", err)
	}
	fmt.Printf("create topic success, topic:[%s],patitions:[%d],replcations:[%d] ", topicName, patitionNum, replicationNum)
	return nil
}

func createTopic(brokers []string, topicName string, partitionNum int32, relicationNum int16) error {
	admin, err := NewClusterAdmin(brokers)
	if err != nil {
		return fmt.Errorf("new admin failed: %w", err)
	}
	defer admin.Close()
	topics, err := admin.ListTopics()
	if err != nil {
		return fmt.Errorf("query topic failed: %w", err)
	}
	if _, exist := topics[topicName]; exist {
		return fmt.Errorf("topic: %s already exist", topicName)
	}
	td := &sarama.TopicDetail{
		NumPartitions:     partitionNum,
		ReplicationFactor: relicationNum,
	}
	return admin.CreateTopic(topicName, td, false)
}
