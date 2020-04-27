package cn.lgwen.kafka.tool;


import org.junit.Test;

public class RunMigrationTest {

    @Test
    public void test() {
        String[] args = new String[]{
                "--topic-source", "collect.s.vul",
                "--group", "nova",
                "--topic-sink","collect.s.vul",
                "--zk-source", "10.50.6.42:12181",
                "--zk-sink", "10.20.128.210:12181"
        };
        RunMigration.main(args);
    }

}