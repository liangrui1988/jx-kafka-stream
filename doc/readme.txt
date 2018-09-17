创建topic 分配两个副本在不同服务上（备用，如果节点挂了就切换），一个分区（可以多个）
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic test2

查看topic列表
bin/kafka-topics.sh --list --zookeeper localhost:2181  

查看topic属性
bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic test 

配置Log Cleaner 日志压缩
log.cleaner.enable=true

启动cleaner线程池，要启动一个特定的topic，你可以添加特定的日志属性：
log.cleanup.policy=compact


查看偏移位置
 bin/kafka-consumer-groups.sh --bootstrap-server broker1:9092 --describe --group test-consumer-group

查看zk状态
bin/zkServer.sh status 
启动 
bin/zkServer.sh start 

测试：
打开三个消费终端
bin/kafka-console-consumer.sh --zookeeper 192.168.20.243:2181 --topic test3 --from-beginning
bin/kafka-console-consumer.sh --zookeeper 192.168.20.243:22181 --topic test3 --from-beginning
bin/kafka-console-consumer.sh --zookeeper 192.168.20.243:32181 --topic test3 --from-beginning
打开-个生产终端
bin/kafka-console-producer.sh --broker-list 192.168.20.243:9092 --topic test3