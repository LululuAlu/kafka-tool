### kafka java工具，解决命令行不足的问题
* GroupOffsetLag
查询group 落后 produce 多少
```shell script
java -cp kafka-tool-[version] cn.lgwen.kafka.tool.GroupOffsetLag  --zk zk.host:port --topic  --group
```
* MessageSize
查询topic 消息总量
```shell script
java -cp kafka-tool-[version] cn.lgwen.kafka.tool.GroupOffsetLag.MessageSize --zk zk.host:port --topic 
```

* ReceiveLatestMessage
拉取最新的消息,size default = 10
```shell script
java -cp kafka-tool-[version] cn.lgwen.kafka.tool.GroupOffsetLag.MessageSize --zk zk.host:port --topic [--size]
```