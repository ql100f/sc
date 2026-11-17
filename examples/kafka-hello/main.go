// kafka-hello 是脱离秒杀业务的最小 Kafka 示例：只演示生产者发布消息和消费者订阅消息。
//
// 运行生产者：
//
//	go run ./examples/kafka-hello -brokers 192.168.0.108:9094 -topic kafka.hello -mode produce -message "hello kafka"
//
// 运行消费者（另开一个终端）：
//
//	go run ./examples/kafka-hello -brokers 192.168.0.108:9094 -topic kafka.hello -mode consume -group kafka-hello-group
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

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		brokers  = flag.String("brokers", "192.168.0.108:9094", "Kafka broker 地址，多个用逗号分隔")
		topic    = flag.String("topic", "kafka.hello", "Kafka topic")
		mode     = flag.String("mode", "produce", "produce 或 consume")
		group    = flag.String("group", "kafka-hello-group", "消费者组，仅 consume 模式使用")
		key      = flag.String("key", "hello-key", "消息 key，仅 produce 模式使用")
		message  = flag.String("message", "hello kafka", "消息 value，仅 produce 模式使用")
		timeout  = flag.Duration("timeout", 10*time.Second, "生产者单次发布超时")
	)
	flag.Parse()

	brokerList := strings.Split(*brokers, ",")

	switch *mode {
	case "produce":
		if err := produce(brokerList, *topic, *key, *message, *timeout); err != nil {
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

// produce 创建一个最小生产者，同步发送一条消息，等待 Kafka 返回成功后退出。
func produce(brokers []string, topic, key, value string, timeout time.Duration) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProduceRequestTimeout(timeout),
	)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer client.Close()

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(value),
	}

	// ProduceSync 同步等待本次生产结果；FirstErr 返回批次中的第一个错误。
	results := client.ProduceSync(context.Background(), record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce record: %w", err)
	}

	fmt.Printf("produced: topic=%s partition=%d offset=%d key=%s value=%s\n",
		topic, results[0].Record.Partition, results[0].Record.Offset, key, value)
	return nil
}

// consume 创建一个最小消费者，按消费组订阅 topic；关闭自动提交，处理成功后手工提交位点。
func consume(brokers []string, topic, group string) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("consumer started", "topic", topic, "group", group)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		// PollRecords 阻塞拉取一批记录，最多 100 条。
		fetches := client.PollRecords(ctx, 100)
		if ctx.Err() != nil {
			return nil
		}

		// 拉取层面的错误，例如 Broker 不可达；这里简单打印并等待下一轮。
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				slog.Error("poll failed",
					"topic", fetchErr.Topic,
					"partition", fetchErr.Partition,
					"error", fetchErr.Err,
				)
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if fetches.NumRecords() == 0 {
			continue
		}

		var records []*kgo.Record
		fetches.EachRecord(func(record *kgo.Record) {
			fmt.Printf("consumed: topic=%s partition=%d offset=%d key=%s value=%s\n",
				record.Topic, record.Partition, record.Offset,
				string(record.Key), string(record.Value))
			records = append(records, record)
		})

		// 演示代码把每条消息都视为处理成功；真实业务应在事务/落库成功后再提交位点。
		if err := client.CommitRecords(ctx, records...); err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
	}
}
