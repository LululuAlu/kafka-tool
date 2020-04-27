package cn.lgwen.kafka.tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 2020/2/26
 * aven.wu
 * danxieai258@163.com
 * topic 数据迁移
 */
public class RunMigration {

    public static void main(String[] args) {
        Command properties = new Command();
        JCommander jCommander = JCommander.newBuilder().addObject(properties).build();
        jCommander.parse(args);

        KafkaConnector sourceConnector = new KafkaConnector(properties.zkSource);
        KafkaConnector sinkConnector = new KafkaConnector(properties.zkSink);
        KafkaProducer<String, String> producer = sinkConnector.kafkaProducer();
        sourceConnector.consumer(properties.sourceTopic, properties.groupId, x -> {
            ProducerRecord<String, String> record = new ProducerRecord<>(properties.sinkTopic, x);
            producer.send(record);
        });

    }



    public static class Command {
        @Parameter(names = {"--topic-source"}, required = true)
        public String sourceTopic;
        @Parameter(names = {"--group"}, required = true)
        public String groupId;
        @Parameter(names = {"--topic-sink"}, required = true)
        public String sinkTopic;
        @Parameter(names = {"--zk-source"}, required = true)
        public String zkSource;
        @Parameter(names = {"--zk-sink"}, required = true)
        public String zkSink;
    }
}
