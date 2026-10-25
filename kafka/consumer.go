package kafka

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sc/internal"
	"syscall"

	"github.com/IBM/sarama"
)

func init() {
	internal.Register(&consumer{})
}

type consumer struct{}

func (c *consumer) Name() string {
	return "kafka-consumer"
}
func (c *consumer) Run(ctx context.Context) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	group := "group1"
	if len(os.Args) >= 4 {
		group = os.Args[3]
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		StartConsumerGroup(ctx, topicName, group)
	}()

	// 信号监听，优雅关闭
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	select {
	case <-ctx.Done():
		cancelFunc()
	case <-sigChan:
		cancelFunc()
	}
	fmt.Println("\n收到退出信号")
	<-done
	return nil
}

type ConsumerGroupHandler struct {
	GroupName string
}

func NewConsumerGroupHandler(groupName string) *ConsumerGroupHandler {
	return &ConsumerGroupHandler{
		GroupName: groupName,
	}
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// 处理消息
		// 这里可以根据需要进行消息处理逻辑
		// 例如打印消息内容
		fmt.Printf("[%s] partition=%d offset=%d value=%s\n",
			h.GroupName, msg.Partition, msg.Offset, string(msg.Value))

		// 标记消息已处理
		session.MarkMessage(msg, "")
		// session.Commit()
	}
	return nil
}

func StartConsumerGroup(ctx context.Context, topicName string, groupID string) {
	consumerGroup, err := NewConsumerGroup(groupID)
	if err != nil {
		fmt.Printf("❌ 创建消费者组失败，%v\n", err)
		return
	}
	defer consumerGroup.Close()
	// ✅ 必须消费error通道，防止sarama内部阻塞
	go func() {
		for err := range consumerGroup.Errors() {
			fmt.Printf("[%s] kafka error: %v\n", groupID, err)
		}
	}()

	handler := NewConsumerGroupHandler(groupID)
	for {
		err := consumerGroup.Consume(ctx, []string{topicName}, handler)
		if ctx.Err() != nil {
			fmt.Printf("❌ 消费者组 %s 停止消费，ctx已取消 %v\n", groupID, ctx.Err())
			return
		}
		if err != nil {
			fmt.Printf("❌ 消费者组 %s 消费失败，%v\n", groupID, err)
			return
		}
	}
}
