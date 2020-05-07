# kafka java tool
## command

* GroupOffsetLag 查询某个group对某个topic 落后多少
```shell script
java -cp kafka-tool-1.0-SNAPSHOT.jar cn.lgwen.kafka.tool.GroupOffsetLag --topic :topic --group :group [--zk : zkHost] [--brokers :broker]
``` 

* MessageSize 查询某个topic的messageSize
```shell script
java -cp kafka-tool-1.0-SNAPSHOT.jar cn.lgwen.kafka.tool.MessageSize --topic :topic [--zk : zkHost] [--brokers :broker]
``` 

* ReceiveLatestMessage 获取最新消息
```shell script
java -cp kafka-tool-1.0-SNAPSHOT.jar cn.lgwen.kafka.tool.ReceiveLatestMessage --topic :topic [--zk : zkHost] [--brokers :broker] [--size :size(default 10)]
``` 