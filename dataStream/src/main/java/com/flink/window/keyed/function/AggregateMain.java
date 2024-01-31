package com.flink.window.keyed.function;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
public class AggregateMain {
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
        
        SingleOutputStreamOperator<WaterSensor> reduce = windowedStream.aggregate(new AggregateFunction<WaterSensor, WaterSensor, WaterSensor>() {
            @Override
            public WaterSensor createAccumulator() {
                System.out.println("创建累加器");
                return new WaterSensor("ss", 0L, 0);
            }
            
            @Override
            public WaterSensor add(WaterSensor waterSensor, WaterSensor accumulator) {
                System.out.println("调用add方法: " + waterSensor);
                return new WaterSensor(waterSensor.getId(), waterSensor.getTs() + accumulator.getTs(), waterSensor.getVc() + accumulator.getVc());
            }
            
            @Override
            public WaterSensor getResult(WaterSensor accumulator) {
                System.out.println("调用getResult方法");
                return accumulator;
            }
            
            @Override
            public WaterSensor merge(WaterSensor a, WaterSensor b) {
                // 只有会话窗口才会使用
                System.out.println("调用merge方法");
                return null;
            }
        });
        
        reduce.print();
        
        env.execute();
    }
}
