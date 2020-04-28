package cn.lgwen.kafka.tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 2020/4/28
 * aven.wu
 * danxieai258@163.com
 * 查询落后多少 lat
 */
public class GroupOffsetLag {


    public static void search(String topic, String groupId, String zkHost) {
        Map<Integer, Long> endOffsetMap = new HashMap<>();

        Properties consumeProps = new KafkaConnector(zkHost).getConsumerProperties();
        consumeProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //查询topic partitions
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumeProps);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionsFor) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
        }

        //查询log size
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
        for (TopicPartition partitionInfo : endOffsets.keySet()) {
            endOffsetMap.put(partitionInfo.partition(), endOffsets.get(partitionInfo));
        }
        // 使用groupId 去拉取数据
        KafkaConnector kafkaConnector = new KafkaConnector(zkHost);
        Properties properties = kafkaConnector.getConsumerProperties();
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");

        Map<Integer, Long> offsetMap = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            KafkaConsumer<String, String> partitionConsumer = new KafkaConsumer<>(properties);
            //partitionConsumer.subscribe(Collections.singleton("nova.ana.vul.state"));
            partitionConsumer.assign(Collections.singleton(topicPartition));
            System.out.print("pool data waite 1s");
            ConsumerRecords<String, String> records = partitionConsumer.poll(1000);
            offsetMap.put(topicPartition.partition(), 0L);
            for (ConsumerRecord<String, String> record : records) {
                // 拉不到数据 就表示不落后
                Long offset = record.offset();
                offsetMap.put(topicPartition.partition(), offset);
                break;
            }
        }
        long lag = 0L;
        for (int partitionId : offsetMap.keySet()) {
            lag = lag + endOffsetMap.get(partitionId) - offsetMap.get(partitionId);
            if (offsetMap.get(partitionId) == 0) {
                System.out.println("no message consumer lag:0");
            }
            System.out.println(String.format("topic:%s, partition:%d, logSize:%d, groupOffset:%d, lag:%d", topic, partitionId, endOffsetMap.get(partitionId), offsetMap.get(partitionId), endOffsetMap.get(partitionId) - offsetMap.get(partitionId)));
        }
        System.out.println("total lag:" + lag);
        consumer.close();
    }

    public static void main(String[] args) {
        Command properties = new Command();
        JCommander jCommander = JCommander.newBuilder().addObject(properties).build();
        jCommander.parse(args);
        search(properties.topic, properties.groupId, properties.zk);
    }

    public static class Command {
        @Parameter(names = {"--group"}, required = true)
        public String groupId;
        @Parameter(names = {"--topic"}, required = true)
        public String topic;
        @Parameter(names = {"--zk"}, required = true)
        public String zk;
    }
}
