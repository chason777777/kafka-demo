package com.chason.kafkaproducer.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by chason on 2018/4/24.17:22
 */
public class ProducerMain {
    public static void main(String[] args) {
        // 配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        // 设置数据key的序列化处理类
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // 设置数据value的序列化处理类
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // 创建生产者实例
        Producer<String,String> producer = new KafkaProducer<String, String>(props);
        // 创建一条新的记录，第一个参数为Topic名称
        ProducerRecord record = new ProducerRecord<String,String>("my-topic","userName","Angus");
        // 发送记录
        producer.send(record);
        producer.close();
    }
}
