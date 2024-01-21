package com.flink.transform.partition;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:}
 */
public class PartitionMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(2);
        
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        
        // DataStream<String> result = dataStreamSource.rescale();
        // DataStream<String> result = dataStreamSource.rebalance();
        // DataStream<String> result = dataStreamSource.global();
        // DataStream<String> result = dataStreamSource.shuffle();
        DataStream<String> result = dataStreamSource.broadcast();
        
        
        result.print();
        
        env.execute();
    }
}
