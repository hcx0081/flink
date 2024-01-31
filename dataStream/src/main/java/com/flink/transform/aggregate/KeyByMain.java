package com.flink.transform.aggregate;

import com.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:}
 */
public class KeyByMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStreamSource<WaterSensor> dataStreamSource = env.fromElements(
                new WaterSensor("1", 1L, 1),
                new WaterSensor("1", 1L, 1),
                new WaterSensor("2", 2L, 2),
                new WaterSensor("2", 2L, 2),
                new WaterSensor("3", 3L, 3),
                new WaterSensor("3", 3L, 3)
        );
        
        KeyedStream<WaterSensor, String> keyedStream = dataStreamSource.keyBy(waterSensor -> {
            if ("1".equals(waterSensor.getId())) {
                return "one";
            } else {
                return "other";
            }
        });
        
        keyedStream.print();
        // 3> WaterSensor(id=2, ts=2, vc=2)
        // 3> WaterSensor(id=2, ts=2, vc=2)
        // 3> WaterSensor(id=3, ts=3, vc=3)
        // 3> WaterSensor(id=3, ts=3, vc=3)
        // 4> WaterSensor(id=1, ts=1, vc=1)
        // 4> WaterSensor(id=1, ts=1, vc=1)
        
        env.execute();
    }
}
