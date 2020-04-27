package cn.lgwen.kafka.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jdk.nashorn.internal.objects.annotations.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * 2020/1/2
 * aven.wu
 * danxieai258@163.com
 * 链接kafka的工具类
 */
public class KafkaConnector {

    private Properties consumerProperties;

    private Properties produceProperties;

    public Properties getConsumerProperties() {
        return consumerProperties;
    }

    public Properties getProduceProperties() {
        return produceProperties;
    }

    public KafkaConnector(String zkHost) {
        try {
            ZooKeeper zk = new ZooKeeper(zkHost, 10000, null);
            List<String> ids = zk.getChildren("/brokers/ids", false);
            List<String> servers = new ArrayList<>();
            ObjectMapper mapper = new ObjectMapper();
            for (String id : ids) {
                String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
                JsonNode jsonNode = mapper.readTree(brokerInfo);
                String host = jsonNode.get("host").asText();
                String port = jsonNode.get("port").asText();
                servers.add(host + ":" + port);
            }
            String kafkaBootstrapServers = String.join(",", servers.toArray(new String[0]));
            consumerProperties = new Properties();
            consumerProperties.setProperty("max.partition.fetch.bytes", "10485760");
            consumerProperties.setProperty("max.poll.records", "50000");
            consumerProperties.setProperty("auto.offset.reset", "earliest");
            consumerProperties.setProperty("bootstrap.servers", kafkaBootstrapServers);
            consumerProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
            consumerProperties.setProperty("value.deserializer",  StringDeserializer.class.getName());


            produceProperties = new Properties();
            produceProperties.setProperty("bootstrap.servers", kafkaBootstrapServers);
            //produceProperties.setProperty("max.request.size", "102400");
            produceProperties.setProperty("controlled.shutdown.max.retries", "2");
            produceProperties.setProperty("controlled.shutdown.retry.backoff.ms", "500");
            produceProperties.setProperty("key.serializer", StringSerializer.class.getName());
            produceProperties.setProperty("value.serializer", StringSerializer.class.getName());

        } catch (IOException | KeeperException | InterruptedException e) {
            throw new RuntimeException("init error", e);
        }
    }

    /**
     * The secondary method will not return
     * @param topic
     * @param groupId
     * @param consumer
     */
    public void consumer(String topic, String groupId, Consumer<String> consumer) {
        final Properties properties = (Properties) consumerProperties.clone();
        properties.setProperty("group.id", groupId);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records;
        while (true) {
            records = kafkaConsumer.poll(100);
            if (records != null && !records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records)
                    consumer.accept(record.value());
            }
        }
    }

    public void consumer(String topic, String groupId, Consumer<String> consumer, int size) {
        final Properties properties = (Properties) consumerProperties.clone();
        properties.setProperty("group.id", groupId);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records;
        int index = 0;
        while (true) {
            if (index >= size) {
                break;
            }
            records = kafkaConsumer.poll(100);
            if (records != null && !records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records)
                    consumer.accept(record.value());
            }
            index++;
        }
    }

    public KafkaProducer<String, String> kafkaProducer() {
        final Properties properties = (Properties) produceProperties.clone();
        return new KafkaProducer<>(properties);
    }

}
