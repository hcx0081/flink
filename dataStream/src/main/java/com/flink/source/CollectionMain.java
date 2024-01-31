package com.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * {@code @description:}
 */
public class CollectionMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStreamSource<Integer> dataStreamSource = env.fromCollection(Arrays.asList(1, 2, 3));
        dataStreamSource.print();
        
        env.execute();
    }
}
