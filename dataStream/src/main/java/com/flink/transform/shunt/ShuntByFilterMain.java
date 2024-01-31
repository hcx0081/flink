package com.flink.transform.shunt;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:}
 */
public class ShuntByFilterMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(2);
        
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        
        SingleOutputStreamOperator<String> odd = dataStreamSource.filter(item -> Integer.parseInt(item) % 2 == 1);
        SingleOutputStreamOperator<String> even = dataStreamSource.filter(item -> Integer.parseInt(item) % 2 == 0);
        
        odd.print("奇数流");
        even.print("偶数流");
        
        env.execute();
    }
}
