package com.practice.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author Altshuler
 */
public class OldProducer {

    public static void main(String[] args) throws InterruptedException {

        //创建客户端
        Properties props = new Properties();
        props.put("metadata.broker.list", "hadoop102:9092");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<Integer,String> producer = new Producer<Integer, String>(config);
        
        //发送数据
        KeyedMessage<Integer, String> message;
        int times = 10;
        for (int i = 0; i < times; i++) {
            message = new KeyedMessage<Integer, String>("first","hello"+i);
            Thread.sleep(1000);
            producer.send(message);
        }
    }
}
