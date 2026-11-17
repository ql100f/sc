// stream-kafka-sarama 是脱离秒杀业务的最小串联示例，Kafka 部分使用 IBM/sarama。
// franz-go 版本保留在 examples/stream-kafka-franz/main.go。
//
//	stream-produce：用 XADD 写入 Redis Stream；
//	relay：用消费组 XREADGROUP 读取 Stream，发布到 Kafka，成功后 XAck + XDel；
//	kafka-consume：用 sarama 订阅 Kafka 并打印消息。
//
// 先启动 relay 和 kafka-consume，再运行 stream-produce：
//
//	go run ./examples/stream-kafka-sarama -mode relay
//	go run ./examples/stream-kafka-sarama -mode kafka-consume
//	go run ./examples/stream-kafka-sarama -mode stream-produce -count 5
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
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		mode          = flag.String("mode", "stream-produce", "stream-produce、relay 或 kafka-consume")
		redisAddr     = flag.String("redis-addr", "192.168.0.108:6379", "Redis 地址")
		redisPassword = flag.String("redis-password", "123456", "Redis 密码")
		redisDB       = flag.Int("redis-db", 0, "Redis 数据库编号")
		stream        = flag.String("stream", "stream.kafka.hello", "Redis Stream key")
		relayGroup    = flag.String("relay-group", "stream-kafka-relay", "Redis Stream 消费组")
		relayConsumer = flag.String("relay-consumer", "relay-"+uuid.NewString(), "Relay 消费者实例名")
		brokers       = flag.String("brokers", "192.168.0.108:9094", "Kafka broker 地址，多个用逗号分隔")
		topic         = flag.String("topic", "stream.kafka.hello", "Kafka topic")
		kafkaGroup    = flag.String("kafka-group", "stream-kafka-sarama-group", "Kafka 消费组")
		count         = flag.Int("count", 5, "stream-produce 写入的消息条数")
		messagePrefix = flag.String("prefix", "msg", "Stream 消息业务 ID 前缀")
		block         = flag.Duration("block", time.Second, "XREADGROUP 阻塞时长")
	)
	flag.Parse()

	brokerList := strings.Split(*brokers, ",")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch *mode {
	case "stream-produce":
		client := newRedisClient(*redisAddr, *redisPassword, *redisDB)
		defer client.Close()
		if err := produceStream(ctx, client, *stream, *messagePrefix, *count); err != nil {
			slog.Error("stream produce failed", "error", err)
			os.Exit(1)
		}
	case "relay":
		redisClient := newRedisClient(*redisAddr, *redisPassword, *redisDB)
		defer redisClient.Close()

		producer, err := newKafkaProducer(brokerList)
		if err != nil {
			slog.Error("create kafka producer failed", "error", err)
			os.Exit(1)
		}
		defer producer.Close()

		if err := runRelay(ctx, redisClient, producer, *stream, *relayGroup, *relayConsumer, *topic, *block); err != nil {
			slog.Error("relay failed", "error", err)
			os.Exit(1)
		}
	case "kafka-consume":
		if err := consumeKafka(ctx, brokerList, *topic, *kafkaGroup); err != nil {
			slog.Error("kafka consume failed", "error", err)
			os.Exit(1)
		}
	default:
		slog.Error("mode must be stream-produce、relay or kafka-consume", "mode", *mode)
		os.Exit(1)
	}
}

// newRedisClient 创建最小 Redis 连接；示例只使用少量连接。
func newRedisClient(addr, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
		PoolSize: 10,
	})
}

// produceStream 用 XADD 向 Stream 追加 N 条演示消息。
func produceStream(ctx context.Context, client *redis.Client, stream, prefix string, count int) error {
	for i := 1; i <= count; i++ {
		messageID := fmt.Sprintf("%s-%d", prefix, i)
		payload := fmt.Sprintf("hello stream #%d", i)

		entryID, err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			ID:     "*", // 让 Redis 自动生成 Stream entry ID
			Values: map[string]any{
				"id":      messageID,
				"payload": payload,
			},
		}).Result()
		if err != nil {
			return fmt.Errorf("xadd stream %s: %w", stream, err)
		}

		fmt.Printf("stream produced: entry=%s id=%s payload=%s\n", entryID, messageID, payload)
	}
	return nil
}

// newKafkaProducer 创建 sarama 同步生产者；Return.Successes 是 SyncProducer 等待成功结果的必要开关。
func newKafkaProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("create sarama producer: %w", err)
	}
	return producer, nil
}

// runRelay 创建 Stream 消费组，循环读取未确认消息；Kafka 发布成功后才 ACK 并删除 entry。
func runRelay(
	ctx context.Context,
	redisClient *redis.Client,
	producer sarama.SyncProducer,
	stream, group, consumer, topic string,
	block time.Duration,
) error {
	// MKSTREAM：Stream 不存在时自动创建；BUSYGROUP 表示消费组已存在，可忽略。
	if err := redisClient.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil &&
		!strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create stream consumer group: %w", err)
	}

	slog.Info("relay started", "stream", stream, "group", group, "consumer", consumer, "topic", topic)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		// ">" 表示读取当前消费组尚未分发的新消息。
		results, err := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    100,
			Block:    block,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("read stream failed", "error", err)
			sleep(ctx, 500*time.Millisecond)
			continue
		}

		for _, result := range results {
			for _, message := range result.Messages {
				messageID, _ := message.Values["id"].(string)
				payload, _ := message.Values["payload"].(string)
				if messageID == "" || payload == "" {
					// 非法演示消息不删除，保留在 PEL 中便于观察；真实项目可进死信队列。
					slog.Error("invalid stream message", "entry", message.ID, "values", message.Values)
					continue
				}

				partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
					Topic: topic,
					Key:   sarama.StringEncoder(messageID),
					Value: sarama.StringEncoder(payload),
				})
				if err != nil {
					// Kafka 发布失败时不 ACK、不删除，消息留在消费组 PEL 等待重试。
					slog.Error("publish kafka failed, message remains pending", "id", messageID, "error", err)
					continue
				}

				pipe := redisClient.TxPipeline()
				pipe.XAck(ctx, stream, group, message.ID)
				pipe.XDel(ctx, stream, message.ID)
				if _, err := pipe.Exec(ctx); err != nil {
					slog.Error("ack and delete stream message failed", "entry", message.ID, "error", err)
					continue
				}

				fmt.Printf("relayed: stream-entry=%s kafka-partition=%d kafka-offset=%d id=%s payload=%s\n",
					message.ID, partition, offset, messageID, payload)
			}
		}
	}
}

// consumeKafka 用 sarama 消费组订阅 Kafka；处理成功后 MarkMessage + Commit。
func consumeKafka(ctx context.Context, brokers []string, topic, group string) error {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	consumer, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		return fmt.Errorf("create sarama consumer group: %w", err)
	}
	defer consumer.Close()

	handler := &kafkaHandler{}
	slog.Info("kafka consumer started", "topic", topic, "group", group)

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
			sleep(ctx, 500*time.Millisecond)
		}
	}
}

// kafkaHandler 实现 sarama.ConsumerGroupHandler。
type kafkaHandler struct{}

// Setup 在消费组会话开始前调用。
func (h *kafkaHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 在消费组会话结束后调用。
func (h *kafkaHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 处理当前消费者分到的分区消息。
func (h *kafkaHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for record := range claim.Messages() {
		fmt.Printf("kafka consumed: topic=%s partition=%d offset=%d key=%s value=%s\n",
			record.Topic, record.Partition, record.Offset, string(record.Key), string(record.Value))

		// MarkMessage 标记当前消息已处理；Commit 把已标记位点提交给 Kafka。
		session.MarkMessage(record, "")
		session.Commit()
	}
	return nil
}

func sleep(ctx context.Context, duration time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(duration):
	}
}
