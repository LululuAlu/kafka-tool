package cn.lgwen.kafka.tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 2020/4/28
 * aven.wu
 * danxieai258@163.com
 */
public class GroupOffsetFetch {



    public static void search(String topic, String groupId, String zkHost) {
        Map<Integer, Long> endOffsetMap = new HashMap<>();
        Map<Integer, Long> commitOffsetMap = new HashMap<>();

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
        for (Integer partitionId : endOffsetMap.keySet()) {
            System.out.println(String.format("at %s, topic:%s, partition:%s, logSize:%s", System.currentTimeMillis(), topic, partitionId, endOffsetMap.get(partitionId)));
        }

        //查询消费offset
        for (TopicPartition topicAndPartition : topicPartitions) {
            OffsetAndMetadata committed = consumer.committed(topicAndPartition);
            commitOffsetMap.put(topicAndPartition.partition(), committed.offset());
        }
        //累加lag
        long lagSum = 0L;
        if (endOffsetMap.size() == commitOffsetMap.size()) {
            for (Integer partition : endOffsetMap.keySet()) {
                long endOffSet = endOffsetMap.get(partition);
                long commitOffSet = commitOffsetMap.get(partition);
                long diffOffset = endOffSet - commitOffSet;
                lagSum += diffOffset;
                System.out.println("Topic:" + topic + ", groupID:" + groupId + ", partition:" + partition + ", endOffset:" + endOffSet + ", commitOffset:" + commitOffSet + ", diffOffset:" + diffOffset);
            }
            System.out.println("Topic:" + topic + ", groupID:" + groupId + ", LAG:" + lagSum);
        } else {
            System.out.println("this topic partitions lost");
        }

        consumer.close();
    }

    public static void main(String[] args) {
        Command properties = new Command();
        JCommander jCommander = JCommander.newBuilder().addObject(properties).build();
        jCommander.parse(args);
        search(properties.topic, properties.groupId,properties.zk);
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
