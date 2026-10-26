package kafka

import (
	"context"
	"fmt"
	"os"
	"sc/internal"
	"time"

	"github.com/IBM/sarama"
)

func init() {
	internal.Register(&producer{})
}

type producer struct{}

func (p *producer) Name() string {
	return "kafka-producer"
}
func (p *producer) Run(ctx context.Context) error {
	note := ""
	if os.Args[3] != "" {
		note = os.Args[3]
	}
	np, err := NewSyncProducer()
	if err != nil {
		return err
	}
	defer np.Close()
	for i := 0; i < 1000; i++ {
		data := fmt.Sprintf("消息%d, note: %s", i, note)
		err := sendMsg(np, topicName, data)
		if err != nil {
			return err
		}
		time.Sleep(time.Second * 1)
	}
	return nil
}

func sendMsg(p sarama.SyncProducer, topic string, data string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		// Key:   sarama.StringEncoder("orderid_152345314"), // 可以指定key，指定Key之后会按 hash(key)%分区总数 路由到具体的分区
		Value: sarama.StringEncoder(data),
	}
	part, offset, err := p.SendMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("✅ 发送消息成功，分区：%d，偏移量：%d\n", part, offset)
	return nil
}
