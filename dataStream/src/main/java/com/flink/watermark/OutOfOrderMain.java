package com.flink.watermark;

import com.flink.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * {@code @description:}
 */
public class OutOfOrderMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        
        SingleOutputStreamOperator<WaterSensor> dataStreamSource = env.socketTextStream("localhost", 8888)
                                                                      .map(new MapFunction<String, WaterSensor>() {
                                                                          @Override
                                                                          public WaterSensor map(String value) throws Exception {
                                                                              String[] datas = value.split(",");
                                                                              return new WaterSensor(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
                                                                          }
                                                                      });
        
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                                                            .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                                                                @Override
                                                                                public long extractTimestamp(WaterSensor waterSensor, long recordTimestamp) {
                                                                                    System.out.println("数据: " + waterSensor + ", 时间戳: " + recordTimestamp);
                                                                                    return waterSensor.getVc() * 1000;
                                                                                }
                                                                            });
        SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(watermarkStrategy);
        
        singleOutputStreamOperator.keyBy(WaterSensor::getId)
                                  // 使用事件时间的滚动窗口
                                  .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                                  .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                                      @Override
                                      public void process(String key, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                          String start = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss");
                                          String end = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss");
                                          
                                          long count = elements.spliterator().estimateSize();
                                          
                                          out.collect("\n" +
                                                  "key: " + key + "\n" +
                                                  "开始时间: " + start + "\n" +
                                                  "结束时间: " + end + "\n" +
                                                  "包含 " + count + " 条数据: " + elements);
                                      }
                                  })
                                  .print();
        
        env.execute();
    }
}
