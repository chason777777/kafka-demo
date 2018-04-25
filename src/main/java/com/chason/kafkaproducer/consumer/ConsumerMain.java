package com.chason.kafkaproducer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.util.CollectionUtils;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by chason on 2018/4/24.17:30
 */
public class ConsumerMain {
    public static void main(String[] args) {
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(3);
        // 配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        // 必须指定消费者组
        props.put("group.id", "test");
        // 设置数据key的序列化处理类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 设置数据value的序列化处理类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 订阅 my-Topic的消息
        consumer.subscribe(Arrays.asList("my-topic"));
        // 到服务器中读取记录
        while (true) {
            // 获取消息
            ConsumerRecords<String, String> records = consumer.poll(5);

            // 丢给线程池处理
            fixedThreadPool.execute(new kafkaThreed(records));

            // 数据量小，休眠5秒钟
            if (records.count() < 5) {
                try {
                    System.out.println("------------休眠5秒钟");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }


    static class kafkaThreed implements Runnable {
        private ConsumerRecords<String, String> records = null;
        public kafkaThreed(ConsumerRecords<String, String> records){
            this.records = records;
        }
        public void run() {
            System.out.println("读取数量:" + records.count());
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("-------------------------------------------------------------------------->>>>>>>>>>>>>>");
                System.out.println("key:" + record.key() + ",value:" + record.value());
                System.out.println("-------------------------------------------------------------------------->>>>>>>>>>>>>>");
            }
        }
    }
}
