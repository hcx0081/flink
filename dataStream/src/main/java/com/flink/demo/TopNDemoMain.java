package com.flink.demo;

import com.flink.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * {@code @description:}
 */
public class TopNDemoMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        SingleOutputStreamOperator<WaterSensor> dataStream = env.socketTextStream("localhost", 8888)
                                                                .map(new MapFunction<String, WaterSensor>() {
                                                                    @Override
                                                                    public WaterSensor map(String value) throws Exception {
                                                                        String[] datas = value.split(",");
                                                                        return new WaterSensor(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
                                                                    }
                                                                })
                                                                .assignTimestampsAndWatermarks(WatermarkStrategy
                                                                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                                                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                                                            @Override
                                                                            public long extractTimestamp(WaterSensor waterSensor, long recordTimestamp) {
                                                                                return waterSensor.getVc() * 1000;
                                                                            }
                                                                        }));
        
        dataStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                  .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                      @Override
                      public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                          HashMap<Long, Long> tsCountMap = new HashMap<>();
                          
                          for (WaterSensor element : elements) {
                              Long ts = element.getTs();
                              if (tsCountMap.containsKey(ts)) {
                                  tsCountMap.put(ts, tsCountMap.get(ts) + 1);
                              } else {
                                  tsCountMap.put(ts, 1L);
                              }
                          }
                          
                          ArrayList<Tuple2<Long, Long>> list = new ArrayList<>();
                          for (Long ts : tsCountMap.keySet()) {
                              list.add(Tuple2.of(ts, tsCountMap.get(ts)));
                          }
                          // 根据count进行排序
                          list.sort(new Comparator<Tuple2<Long, Long>>() {
                              @Override
                              public int compare(Tuple2<Long, Long> o1, Tuple2<Long, Long> o2) {
                                  return (int) (o2.f1 - o1.f1);
                              }
                          });
                          
                          StringBuilder str = new StringBuilder();
                          str.append("========\n");
                          for (int i = 0; i < Math.min(2, list.size()); i++) {
                              Tuple2<Long, Long> vcAndCount = list.get(i);
                              str.append("Top" + (i + 1) +
                                      ", vc: " + vcAndCount.f0 +
                                      ", count: " + vcAndCount.f1 +
                                      ", 窗口开始时间: " + DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS") +
                                      ", 窗口结束时间: " + DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS"));
                              str.append("\n");
                          }
                          str.append("========");
                          
                          out.collect(str.toString());
                      }
                  })
                  .print();
        
        env.execute();
    }
}
