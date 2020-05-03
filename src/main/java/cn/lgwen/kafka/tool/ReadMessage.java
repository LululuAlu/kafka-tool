package cn.lgwen.kafka.tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author aven danxieai@163.com
 * @version 0.1
 * @date 2020/4/30
 */
public class ReadMessage {

    public static void main(String[] args) {
        Command properties = new Command();
        JCommander jCommander = JCommander.newBuilder().addObject(properties).build();
        jCommander.parse(args);

        KafkaConnector kafkaConnector;
        if (properties.brokers != null) {
            kafkaConnector = new KafkaConnector(new String[]{properties.brokers});
        } else {
            kafkaConnector = new KafkaConnector(properties.zk);
        }
        Properties consumerProperties = kafkaConnector.getConsumerProperties();
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.setProperty("max.poll.records", "20");
        if (properties.group != null) {
            consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, properties.group);
        } else {
            consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.currentTimeMillis() + "consumer");
        }
        readEarlyMessage(consumerProperties, properties.topic);

    }

    public static void readEarlyMessage(Properties properties, String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        a:while (true) {
            consumer.subscribe(Collections.singleton(topic));
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
                break a;
            }
        }
    }

    public static class Command {
        @Parameter(names = {"--topic"}, required = true)
        public String topic;
        @Parameter(names = {"--group"})
        public String group;
        @Parameter(names = {"--zk"})
        public String zk;
        @Parameter(names = {"--brokers"})
        public String brokers;
    }
    // --topic pbc --brokers localhost:9092
}
