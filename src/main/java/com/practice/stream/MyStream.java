package com.practice.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @author Altshuler 
 */
public class MyStream {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"MyStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        TopologyBuilder topologyBuilder = new TopologyBuilder().addSource("source","one","two")
                .addProcessor("processor",new MySupplier(),"source")
                .addSink("sink","three","processor");
        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder, props);
        kafkaStreams.start();
    }
    
}
