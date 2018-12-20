package com.practice.patitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @author Altshuler
 * 自定义分区
 */
public class CustomerPartitioner implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(s);
        return Math.abs(bytes1.hashCode()) % partitionInfos.size();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
