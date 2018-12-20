package com.practice.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author Altshuler
 */
public class NewProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop102:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 重试最大次数
        props.put("retries", 0);
        // 批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        //// 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        //// 设置分区
        // props.put("partitioner.class", "com.practice.patitioner.CustomerPartitioner");
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<Integer,String> producer = new KafkaProducer<Integer, String>(props);
        int times = 1000;
        for (int i = 0; i < times; i++) {

            ProducerRecord<Integer,String> producerRecord = new ProducerRecord<Integer, String>("first","hello"+i);
            producer.send(producerRecord,(recordMetadata, e) -> {
                if(e != null) {
                    e.printStackTrace();
                    System.out.println("发送失败");
                }else{
                    System.out.println("所在分区:"+recordMetadata.partition());
                    System.out.println("偏移量:"+recordMetadata.offset());
                }
            });
            producer.flush();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        producer.close();
    }
}
