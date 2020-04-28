package cn.lgwen.kafka.tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import kafka.coordinator.group.GroupMetadataManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 2020/4/27
 * aven.wu
 * danxieai258@163.com
 */
public class OffsetMonitor {

    public static void lastCommitTimestamp(String groupName, String zkHost, String topic) {
        int partition = Math.abs(groupName.hashCode() % 50); // Assume offset topic partition number is 50
        TopicPartition tp = new TopicPartition("__consumer_offsets", partition);
        KafkaConnector kafkaConnector = new KafkaConnector(zkHost);

        Properties properties = kafkaConnector.getConsumerProperties();
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        properties.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.setProperty("value.deserializer",  ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(Collections.singletonList(tp));
            consumer.poll(100); // take effect
            Map<TopicPartition, Long> endOffset = consumer.endOffsets(Collections.singletonList(tp));
            consumer.seek(tp, endOffset.get(tp) - 10);
            ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
            if (records.count() == 0) {
                throw new IllegalStateException("Consumer group does not commit offsets.");
            }
            GroupMetadataManager.OffsetsMessageFormatter formatter = new GroupMetadataManager.OffsetsMessageFormatter();
            for (ConsumerRecord<byte[], byte[]> record : records) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                PrintStream printStream = new PrintStream(outputStream);
                formatter.writeTo(record, printStream);
                byte[] bytes = outputStream.toByteArray();
                String string = new String(bytes, StandardCharsets.UTF_8);
                System.out.println(string);
                int idx = 0;
                List<Character> characters = new LinkedList<>();
                for (char c : string.toCharArray()) {
                    if ('[' == c) {
                        continue;
                    }
                    characters.add(c);
                    if (']' == c && idx == 0) {
                        // 第一个框
                        // 可以提取topic
                        String[] ars = charArrayToString(characters).split(",");
                        if (ars[1].equals(topic)) {
                            // 获取到了对应的topic
                        }
                        idx++;
                    }
                }
                outputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String charArrayToString(List<Character> list) {
        char[] chars = new char[list.size()];
        int idx = 0;
        for (Character character : list) {
            chars[idx] = character;
            idx++;
        }
        return new String(chars);
    }

    public static void main(String[] args) {
        Command properties = new Command();
        JCommander jCommander = JCommander.newBuilder().addObject(properties).build();
        jCommander.parse(args);
        lastCommitTimestamp(properties.group, properties.zk, properties.topic);
    }


    public static class Command {
        @Parameter(names = {"--topic"})
        public String topic;
        @Parameter(names = {"--group"}, required = true)
        public String group;
        @Parameter(names = {"--zk"}, required = true)
        public String zk;
    }
}
