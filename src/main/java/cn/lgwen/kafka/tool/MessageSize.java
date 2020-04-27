package cn.lgwen.kafka.tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MessageSize {

    public static void main(String[] args) {
        Command properties = new Command();
        JCommander jCommander = JCommander.newBuilder().addObject(properties).build();
        jCommander.parse(args);
        System.out.println(getTopicOffset(properties.topic, properties.zk));
    }


    public static long getTopicOffset(String topic, String zookeeper) {
        long sum = 0;
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        KafkaConsumer consumer = null;
        try {
            consumer = new KafkaConsumer(new KafkaConnector(zookeeper).getConsumerProperties());

            Map<String, List<PartitionInfo>> topicPartition = consumer.listTopics();

            if (null == topicPartition || topicPartition.size() == 0) {
                return 0L;
            }
            List<PartitionInfo> partitionInfos = topicPartition.get(topic);
            System.out.println("partitionInfo Size:" + partitionInfos.size());

            for (PartitionInfo item : partitionInfos) {
                System.out.println("partitionInfos:" + item);
                topicPartitionList.add(new TopicPartition(topic, item.partition()));
            }
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitionList);
            for (Long endOffset : endOffsets.values()) {
                sum += endOffset;
            }

            System.out.println("Offset sum:" + sum);
        } catch (Exception e) {
            System.out.println("Error communicating with Broker to find Leader for [" + topic + ", ] Reason: " + e);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return sum;
    }


    public static class Command {
        @Parameter(names = {"--topic"}, required = true)
        public String topic;
        @Parameter(names = {"--zk"}, required = true)
        public String zk;
    }


}