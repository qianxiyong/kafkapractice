package com.practice.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author Altshuler
 * 添加过滤器用于计数
 */
public class CountorInterceptor implements ProducerInterceptor<String,String>{

    private long sendError;
    private long sendSuccess;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

        if(e != null) {
            sendSuccess++;
        }else{
            sendError++;
        }

    }

    @Override
    public void close() {
        System.out.println("发送成功的次数为："+sendSuccess);
        System.out.println("发送失败的次数为："+sendError);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
