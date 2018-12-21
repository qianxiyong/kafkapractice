package com.practice.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * @author Altshuler
 * 提供自定义的处理器
 */
public class MySupplier implements ProcessorSupplier {
    @Override
    public Processor get() {
        return new Processor<byte[],byte[]>(){

            private ProcessorContext context;

            @Override
            public void init(ProcessorContext processorContext) {
                this.context = processorContext;
            }

            @Override
            public void process(byte[] key, byte[] value) {
                value = new String(value).replaceAll(">>>","").getBytes();
                context.forward(key,value);
            }

            @Override
            public void punctuate(long l) {

            }

            @Override
            public void close() {

            }
        };
    }

}
