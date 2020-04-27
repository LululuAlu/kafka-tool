package cn.lgwen.kafka.tool;

import org.junit.Test;

/**
 * 2020/3/24
 * aven.wu
 * danxieai258@163.com
 */
public class MyConsumer {

    @Test
    public void test() {
        KafkaConnector connector = new KafkaConnector("10.20.128.210:12181");
        connector.consumer("collect.ga.tsgz.asset.website", "nova0", x -> {
            System.out.println(x);
        });
    }
}
