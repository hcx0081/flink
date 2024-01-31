package com.flink.transform.transform;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * {@code @description:}
 */
public class FlatMapMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStreamSource<WaterSensor> dataStreamSource = env.fromElements(
                new WaterSensor("1", 1L, 1),
                new WaterSensor("2", 2L, 2),
                new WaterSensor("3", 3L, 3)
        );
        
        SingleOutputStreamOperator<String> map = dataStreamSource.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor waterSensor, Collector<String> out) throws Exception {
                String id = waterSensor.getId();
                if ("1".equals(id)) {
                    out.collect("one");
                } else if ("2".equals(id)) {
                    out.collect("two");
                    out.collect("two");
                }
            }
        });
        
        map.print();
        // one
        // two
        // two
        
        env.execute();
    }
}
