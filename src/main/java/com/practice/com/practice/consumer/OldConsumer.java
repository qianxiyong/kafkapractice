package com.practice.com.practice.consumer;

import kafka.api.FetchRequestBuilder;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * @author Altshuler
 * 练习使用低阶kafka消费者 API
 * 需要自己去寻找 要读取主题分区的leader所在位置 然后从该主机读取 读取的时候指定好topic 分区 偏移量
 * 偏移量需要自己维护 可以维护在内存中 也可以维护在文件中
 */
public class OldConsumer {

    public static void main(String[] args) {

        //定义broker
        List<String> brokers = Arrays.asList("hadoop101", "hadoop102", "hadoop103");
        //定义端口
        int port = 9092;
        //定义topic
        String topic = "first";
        //定义分区
        int partition = 0;
        //定义偏移量
        int offset = 0;
        OldConsumer oldConsumer = new OldConsumer();
        oldConsumer.getData(brokers, port, topic, partition, offset);
    }

    /**
     * 获取数据
     *
     * @param brokers
     * @param port
     * @param topic
     * @param partition
     * @param offset
     */
    private void getData(List<String> brokers, int port, String topic, int partition, int offset) {
        String leader = findLeader(brokers, port, topic, partition);
        System.out.println(leader);
        SimpleConsumer consumer = new SimpleConsumer(leader, port, 100 * 1000, 100 * 1024, "lookUpData");
        boolean flag = true;
        while (flag) {
            System.out.println(offset);
            kafka.api.FetchRequest request = new FetchRequestBuilder().addFetch(topic, partition, offset, 100 * 1024).build();
            System.out.println(request);
            FetchResponse response = consumer.fetch(request);
            ByteBufferMessageSet messageAndOffsets = response.messageSet(topic, partition);
            for (MessageAndOffset messageAndOffset : messageAndOffsets) {

                ByteBuffer buffer = messageAndOffset.message().payload();
                byte[] bytes = new byte[buffer.limit()];
                buffer.get(bytes);
                System.out.println("读取的内容是：" + new String(bytes));
                //手动维持offset
                offset = (int)messageAndOffset.offset();
                offset++;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 寻找leader
     *
     * @param brokers
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    private String findLeader(List<String> brokers, int port, String topic, int partition) {
        //遍历每一台主机
        for (String broker : brokers) {
            SimpleConsumer consumer = new SimpleConsumer(broker, port, 100 * 1000, 100 * 1024, "lookUpLeader");
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Arrays.asList(topic));
            TopicMetadataResponse response = consumer.send(topicMetadataRequest);
            List<TopicMetadata> topicMetadata = response.topicsMetadata();
            //遍历主题元数据信息
            if (topicMetadata != null && topicMetadata.size() > 0) {
                for (TopicMetadata topicMetadatum : topicMetadata) {
                    List<PartitionMetadata> partitionMetadata = topicMetadatum.partitionsMetadata();
                    //遍历分区元数据信息
                    if (partitionMetadata != null && partitionMetadata.size() > 0) {
                        for (PartitionMetadata partitionMetadatum : partitionMetadata) {
                            if (partitionMetadatum.partitionId() == partition) {
                                return partitionMetadatum.leader().host();
                            }
                        }
                    }
                }
            }
        }
        return null;
    }
}
