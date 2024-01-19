package com.flink.transform.aggregate;

import com.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:}
 */
public class MinAndMinByMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStreamSource<WaterSensor> dataStreamSource = env.fromElements(
                new WaterSensor("1", 1L, 1),
                new WaterSensor("1", 2L, 2),
                new WaterSensor("2", 2L, 2),
                new WaterSensor("2", 4L, 4),
                new WaterSensor("3", 3L, 3),
                new WaterSensor("3", 6L, 6)
        );
        
        KeyedStream<WaterSensor, String> keyedStream = dataStreamSource.keyBy(waterSensor -> waterSensor.getId());
        
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.min("vc");
        // 2> WaterSensor(id=1, ts=1, vc=1)
        // 2> WaterSensor(id=1, ts=1, vc=1)
        // 1> WaterSensor(id=2, ts=2, vc=2)
        // 1> WaterSensor(id=2, ts=2, vc=2)
        // 2> WaterSensor(id=3, ts=3, vc=3)
        // 2> WaterSensor(id=3, ts=3, vc=3)
        
        // SingleOutputStreamOperator<WaterSensor> result = keyedStream.minBy("vc");
        // 2> WaterSensor(id=1, ts=1, vc=1)
        // 2> WaterSensor(id=1, ts=1, vc=1)
        // 1> WaterSensor(id=2, ts=2, vc=2)
        // 1> WaterSensor(id=2, ts=2, vc=2)
        // 2> WaterSensor(id=3, ts=3, vc=3)
        // 2> WaterSensor(id=3, ts=3, vc=3)
        
        result.print();
        
        env.execute();
    }
}
