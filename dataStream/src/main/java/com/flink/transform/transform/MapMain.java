package com.flink.transform.transform;

import com.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:}
 */
public class MapMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStreamSource<WaterSensor> dataStreamSource = env.fromElements(
                new WaterSensor("1", 1L, 1),
                new WaterSensor("2", 2L, 2),
                new WaterSensor("3", 3L, 3)
        );
        
        SingleOutputStreamOperator<String> map = dataStreamSource.map(waterSensor -> waterSensor.getId());
        
        map.print();
        // 1
        // 2
        // 3
        
        env.execute();
    }
}
