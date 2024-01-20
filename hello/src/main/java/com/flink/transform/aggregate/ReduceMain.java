package com.flink.transform.aggregate;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:}
 */
public class ReduceMain {
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

        SingleOutputStreamOperator<WaterSensor> result = keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor1, WaterSensor waterSensor2) throws Exception {
                return new WaterSensor(waterSensor1.getId(), waterSensor1.getTs() + waterSensor2.getTs(), waterSensor1.getVc() + waterSensor2.getVc());
            }
        });

        result.print();

        env.execute();
    }
}
