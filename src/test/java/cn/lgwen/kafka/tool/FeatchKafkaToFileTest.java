package cn.lgwen.kafka.tool;

import org.junit.Test;

import java.io.IOException;


public class FeatchKafkaToFileTest {
    @Test
    public void test() throws IOException {
        String[] args = new String[]{
                "--topic", "collect.ga.tsgz.event",
                "--group", "nova",
                "--zk", "10.20.128.210:12181",
                "--path", "event.json",
                "--size", "50000"
        };
        FeatchKafkaToFile.main(args);
    }

    @Test
    public void test1() throws IOException {
        String[] args = new String[]{
                "--topic", "",
                "--group", "nova2",
                "--zk", "10.20.128.210:12181",
                "--path", "event.json",
                "--size", "20"
        };
        FeatchKafkaToFile.main(args);
    }
}