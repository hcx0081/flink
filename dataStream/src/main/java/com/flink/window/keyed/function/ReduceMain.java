package com.flink.window.keyed.function;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * {@code @description:}
 */
public class ReduceMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        
        SingleOutputStreamOperator<WaterSensor> dataDS = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
            }
        });
        
        KeyedStream<WaterSensor, String> dataKS = dataDS.keyBy(WaterSensor::getId);
        
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = dataKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        
        SingleOutputStreamOperator<WaterSensor> reduce = windowedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor1, WaterSensor waterSensor2) throws Exception {
                return new WaterSensor(waterSensor1.getId(), waterSensor1.getTs() + waterSensor2.getTs(), waterSensor1.getVc() + waterSensor2.getVc());
            }
        });
        
        reduce.print();
        
        env.execute();
    }
}
