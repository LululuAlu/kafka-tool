package cn.lgwen.kafka.tool;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.function.Consumer;

/**
 * 2020/4/3
 * aven.wu
 * danxieai258@163.com
 */
public class WriteDataTOKafka {

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = new KafkaConnector("10.20.128.210:12181").kafkaProducer();
        readData("G:\\tmp\\rule\\kafka_dws_alarm.job", x -> {
            producer.send(
                    new ProducerRecord<>("nova.ana.dandelion.broadcast", x)
            );
        });
        producer.flush();
        producer.close();
    }

    private static void readData(String fileName, Consumer<String> consumer) throws Exception{
        InputStream inputStream = new FileInputStream(new File(fileName));
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null) {
            consumer.accept(line);
        }
    }
}
