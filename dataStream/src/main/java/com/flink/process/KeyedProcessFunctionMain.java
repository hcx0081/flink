package com.flink.process;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * {@code @description:}
 */
public class KeyedProcessFunctionMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        SingleOutputStreamOperator<WaterSensor> dataStream = env.socketTextStream("localhost", 8888)
                                                                .map(new MapFunction<String, WaterSensor>() {
                                                                    @Override
                                                                    public WaterSensor map(String value) throws Exception {
                                                                        String[] datas = value.split(",");
                                                                        return new WaterSensor(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
                                                                    }
                                                                })
                                                                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                                                                                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                                                                                                    @Override
                                                                                                                    public long extractTimestamp(WaterSensor waterSensor, long recordTimestamp) {
                                                                                                                        return waterSensor.getVc() * 1000;
                                                                                                                    }
                                                                                                                }));
        
        KeyedStream<WaterSensor, String> keyedStream = dataStream.keyBy(waterSensor -> waterSensor.getId());
        
        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                String key = ctx.getCurrentKey();
                Long ts = ctx.timestamp();
                // 定时器
                TimerService timerService = ctx.timerService();
                // timerService.registerEventTimeTimer(System.currentTimeMillis() + 5000);
                timerService.registerProcessingTimeTimer(System.currentTimeMillis() + 5000);
                System.out.println("key: " + key + ", 时间戳: " + ts + ", 注册一个定时器");
            }
            
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                System.out.println("定时器触发");
            }
        });
        
        process.print();
        
        env.execute();
    }
}
