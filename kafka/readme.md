### 说明
生产+消费
支持多生产 + 多消费

> 生产者 + Subscribe 消费组模式：**两边都不需要手动写分区号**
> 
> - 生产者：由 kafka 客户端根据 key（hash）/ 无 key（轮询）自动选分区
> - Subscribe 消费组：coordinator 自动做分区分配、rebalance
> 
> Assign 不属于消费组模式，是独立消费者：**必须自己明确指定 topic + 分区号**，没有协调器帮你分配，新增分区它也不会自动感知。

注意：生产者**也支持手动指定分区**（Message.Partition 填数字），这是可选能力，业务上几乎不用；一旦手动写死 Partition，key 的 hash 路由逻辑直接失效。


### 使用方式
生成：
    go run main.go -s kafka-producer node1
    go run main.go -s kafka-producer node2
消费：
    go run main.go -s kafka-consumer group1
    go run main.go -s kafka-consumer group2



### 相关命令
# 查看本地客户端工具版本（本地bin包版本，不一定等于远端broker）
kafka-topics.sh --version
# 连接集群，查看broker支持的API版本（用来做客户端版本兼容判断）
kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:9092
# 之前整理的topic命令放一起，保持一套
kafka-topics.sh --create --bootstrap-server 127.0.0.1:9092 --topic test-topic --partitions 4 --replication-factor 2
kafka-topics.sh --alter --bootstrap-server 127.0.0.1:9092 --topic test-topic --partitions 8
kafka-topics.sh --describe --bootstrap-server 127.0.0.1:9092 --topic test-topic
kafka-topics.sh --delete --bootstrap-server 127.0.0.1:9092 --topic test-topic
