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

    /**
     *
     * @param recordMetadata
     * @param e 在回调的线程中调用 运用在send()方法的回调函数之前 拦截器的调用顺序依赖于添加顺序
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

        if(e != null) {
            System.out.println(e);
            sendError++;
        }else{
            sendSuccess++;
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
