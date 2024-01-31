package com.flink.transform.confluence;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * {@code @description:}
 */
public class ConnectMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        
        DataStreamSource<Integer> dataStreamSource1 = env.fromElements(1, 2, 3);
        DataStreamSource<String> dataStreamSource2 = env.fromElements("4", "5", "6");
        
        ConnectedStreams<Integer, String> connect = dataStreamSource1.connect(dataStreamSource2);
        
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }
            
            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });
        
        map.print();
        // 1
        // 4
        // 2
        // 5
        // 3
        // 6
        
        env.execute();
    }
}
