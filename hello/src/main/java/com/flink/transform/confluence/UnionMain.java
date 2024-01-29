package com.flink.transform.confluence;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:}
 */
public class UnionMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        
        DataStreamSource<Integer> dataStreamSource1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> dataStreamSource2 = env.fromElements(4, 5, 6);
        DataStreamSource<Integer> dataStreamSource3 = env.fromElements(7, 8, 9);
        
        DataStream<Integer> union = dataStreamSource1.union(dataStreamSource2, dataStreamSource3);
        
        union.print();
        // 1
        // 2
        // 3
        // 4
        // 5
        // 6
        // 7
        // 8
        // 9
        
        env.execute();
    }
}
