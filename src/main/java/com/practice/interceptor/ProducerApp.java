package com.practice.interceptor;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Altshuler
 * 使用两个过滤器
 */
public class ProducerApp {

    public static void main(String[] args) {
        //配置信息
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
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 2. 构建拦截器
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.practice.interceptor.TimeInterceptor");
        interceptors.add("com.practice.interceptor.CountorInterceptor");
        props.put("interceptor.classes", interceptors);

        Producer<String,String> producer = new KafkaProducer<String, String>(props);

        int times = 10;
        for(int i = 0;i < times;i++) {
            ProducerRecord<String,String> record = new ProducerRecord<>("first","helloworld:"+i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("消息所在分区："+recordMetadata.partition());
                }
            });
            producer.flush();
        }
        producer.close();
    }
}
