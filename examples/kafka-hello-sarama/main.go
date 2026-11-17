// kafka-hello-sarama 是使用 IBM/sarama 的最小 Kafka 示例：只演示生产者发布和消费者订阅。
// 原 franz-go 版本保留在 examples/kafka-hello/main.go。
//
// 运行生产者：
//
//	go run ./examples/kafka-hello-sarama -brokers 192.168.0.108:9094 -topic kafka.hello -mode produce -message "hello sarama"
//
// 运行消费者（另开一个终端）：
//
//	go run ./examples/kafka-hello-sarama -brokers 192.168.0.108:9094 -topic kafka.hello -mode consume -group kafka-hello-sarama-group
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	var (
		brokers = flag.String("brokers", "192.168.0.108:9094", "Kafka broker 地址，多个用逗号分隔")
		topic   = flag.String("topic", "kafka.hello", "Kafka topic")
		mode    = flag.String("mode", "produce", "produce 或 consume")
		group   = flag.String("group", "kafka-hello-sarama-group", "消费者组，仅 consume 模式使用")
		key     = flag.String("key", "hello-key", "消息 key，仅 produce 模式使用")
		message = flag.String("message", "hello sarama", "消息 value，仅 produce 模式使用")
	)
	flag.Parse()

	brokerList := strings.Split(*brokers, ",")

	switch *mode {
	case "produce":
		if err := produce(brokerList, *topic, *key, *message); err != nil {
			slog.Error("produce failed", "error", err)
			os.Exit(1)
		}
	case "consume":
		if err := consume(brokerList, *topic, *group); err != nil {
			slog.Error("consume failed", "error", err)
			os.Exit(1)
		}
	default:
		slog.Error("mode must be produce or consume", "mode", *mode)
		os.Exit(1)
	}
}

// produce 使用 sarama 同步生产者发送一条消息，等待 Kafka 返回成功后退出。
func produce(brokers []string, topic, key, value string) error {
	config := sarama.NewConfig()
	// WaitForAll 表示等待所有 ISR 副本确认后返回。
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	// SyncProducer 必须开启 Return.Successes，否则 SendMessage 不会等待成功结果。
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	record := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	partition, offset, err := producer.SendMessage(record)
	if err != nil {
		return fmt.Errorf("send message: %w", err)
	}

	fmt.Printf("produced: topic=%s partition=%d offset=%d key=%s value=%s\n",
		topic, partition, offset, key, value)
	return nil
}

// consume 使用 sarama 消费组订阅 topic；关闭自动提交，处理成功后手工 MarkMessage + Commit。
func consume(brokers []string, topic, group string) error {
	config := sarama.NewConfig()
	// OffsetOldest 表示没有已提交位点时从最早消息开始消费。
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// 关闭自动提交，由示例代码在消息处理成功后提交位点。
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	consumer, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		return fmt.Errorf("create consumer group: %w", err)
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	handler := &helloHandler{}
	slog.Info("consumer started", "topic", topic, "group", group)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		// Consume 会在重平衡、会话结束或报错时返回；外层循环重新加入消费组。
		if err := consumer.Consume(ctx, []string{topic}, handler); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("consume session failed", "error", err)
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// helloHandler 实现 sarama.ConsumerGroupHandler。
type helloHandler struct{}

// Setup 在消费组会话开始前调用。
func (h *helloHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 在消费组会话结束后调用。
func (h *helloHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 处理当前消费者分到的分区消息。
func (h *helloHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for record := range claim.Messages() {
		fmt.Printf("consumed: topic=%s partition=%d offset=%d key=%s value=%s\n",
			record.Topic, record.Partition, record.Offset,
			string(record.Key), string(record.Value))

		// MarkMessage 标记当前消息已处理；Commit 把已标记位点提交给 Kafka。
		session.MarkMessage(record, "")
		session.Commit()
	}
	return nil
}
