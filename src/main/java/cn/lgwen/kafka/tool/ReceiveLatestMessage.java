package cn.lgwen.kafka.tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;

/**
 * 2020/4/27
 * aven.wu
 * danxieai258@163.com
 * 获取最新的消息
 */
public class ReceiveLatestMessage {


    public static void receiveLatestMessage(
            Properties properties, String topic, Integer count) {
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        AdminClient adminClient = AdminClient.create(properties);
        try {
            DescribeTopicsResult topicResult = adminClient.describeTopics(Collections.singletonList(topic));
            Map<String, KafkaFuture<TopicDescription>> descMap = topicResult.values();
            Iterator<Map.Entry<String, KafkaFuture<TopicDescription>>> itr = descMap.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry<String, KafkaFuture<TopicDescription>> entry = itr.next();
                List<TopicPartitionInfo> topicPartitionInfoList = entry.getValue().get().partitions();
                for (TopicPartitionInfo topicPartitionInfo : topicPartitionInfoList) {
                    consumerAction(topicPartitionInfo, consumer, topic, count);
                }
            }
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(record.value());
                    }
                    break;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            adminClient.close();
            consumer.close();
        }
    }

    private static void consumerAction(TopicPartitionInfo topicPartitionInfo, KafkaConsumer<String, String> consumer, String topic, Integer count) {
        int partitionId = topicPartitionInfo.partition();
        Node node = topicPartitionInfo.leader();
        TopicPartition topicPartition = new TopicPartition(topic, partitionId);
        Map<TopicPartition, Long> mapBeginning = consumer.beginningOffsets(Collections.singletonList(topicPartition));
        Iterator<Map.Entry<TopicPartition, Long>> beginIterator = mapBeginning.entrySet().iterator();
        long beginOffset = 0;
        while (beginIterator.hasNext()) {
            Map.Entry<TopicPartition, Long> beginEntry = beginIterator.next();
            beginOffset = beginEntry.getValue();
        }
        Map<TopicPartition, Long> mapEnd = consumer.endOffsets(Collections.singletonList(topicPartition));
        Iterator<Map.Entry<TopicPartition, Long>> endIterator = mapEnd.entrySet().iterator();
        long lastOffset = 0;
        while (endIterator.hasNext()) {
            Map.Entry<TopicPartition, Long> endEntry = endIterator.next();
            lastOffset = endEntry.getValue();
        }
        long expectedOffSet = lastOffset - count;
        expectedOffSet = expectedOffSet > 0 ? expectedOffSet : 1;
        consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(expectedOffSet)));
    }

    public static void main(String[] args) throws Exception {

        Command properties = new Command();
        JCommander jCommander = JCommander.newBuilder().addObject(properties).build();
        jCommander.parse(args);

        KafkaConnector kafkaConnector = new KafkaConnector(properties.zk);
        Properties consumerProperties = kafkaConnector.getConsumerProperties();
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.currentTimeMillis() + "consumer");
        receiveLatestMessage(consumerProperties, properties.topic, properties.size);

    }

    public static class Command {
        @Parameter(names = {"--topic"}, required = true)
        public String topic;
        @Parameter(names = {"--zk"}, required = true)
        public String zk;
        @Parameter(names = {"--size"})
        public int size = 10;
    }

}
