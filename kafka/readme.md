### 说明
生产+消费
支持多生产 + 多消费

生成：
    go run main.go -s kafka-producer node1
    go run main.go -s kafka-producer node2
消费：
    go run main.go -s kafka-consumer group1
    go run main.go -s kafka-consumer group2


### 其它
还可以改造下生产，搞成多种生产