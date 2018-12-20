package com.practice.com.practice.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author Altshuler
 * kafka 消费者高级API
 * 提供kafka集群的一个地址即可 然后订阅主题 可以自动确定分区leader的位置  自动确定offset开始读取
 * 偏移量维护在zookeeper中
 */
public class NewConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上  只需要指定一个即可
        props.put("bootstrap.servers", "hadoop102:9092");
        // 指定 group.id
        props.put("group.id", "test");
        // 是否自动确认 offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key 序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value 的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<Integer,String> consumer = new KafkaConsumer<Integer, String>(props);
        consumer.subscribe(Arrays.asList("first"));
        boolean flag = true;
        while (flag) {
            ConsumerRecords<Integer, String> consumerRecords = consumer.poll(400);
            for (ConsumerRecord<Integer, String> consumerRecord : consumerRecords) {
                System.out.printf("offset=%d,partition=%s,value=%s%n",consumerRecord.offset(),consumerRecord.partition(),consumerRecord.value());
            }
        }
    }
}
