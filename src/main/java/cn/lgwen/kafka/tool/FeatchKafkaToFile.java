package cn.lgwen.kafka.tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 2020/3/4
 * aven.wu
 * danxieai258@163.com
 */
public class FeatchKafkaToFile {


    public static void main(String[] args) throws IOException {
        Command properties = new Command();
        JCommander jCommander = JCommander.newBuilder().addObject(properties).build();
        jCommander.parse(args);

        KafkaConnector sourceConnector = new KafkaConnector(properties.zk);
        File file = new File(properties.filePath);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        sourceConnector.consumer(properties.topic, properties.groupId, data -> {
            try {
                writer.append(data);
                writer.append("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }

        }, properties.size);
    }


    public static class Command {
        @Parameter(names = {"--topic"}, required = true)
        public String topic;
        @Parameter(names = {"--group"}, required = true)
        public String groupId;
        @Parameter(names = {"--zk"}, required = true)
        public String zk;
        @Parameter(names = {"--path"}, required = true)
        public String filePath;
        @Parameter(names = {"--size"})
        public Integer size;
    }
}
