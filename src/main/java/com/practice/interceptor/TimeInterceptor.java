package com.practice.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author Altshuler
 * 为每个发送的value添加时间戳
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return new ProducerRecord<String,String>(
                producerRecord.topic(),
                producerRecord.partition(),
                producerRecord.key(),
                System.currentTimeMillis()+" : "+producerRecord.value()
        );
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
