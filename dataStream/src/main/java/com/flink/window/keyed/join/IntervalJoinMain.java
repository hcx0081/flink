package com.flink.window.keyed.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * {@code @description:}
 */
public class IntervalJoinMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(
                                                                             Tuple2.of("a", 1),
                                                                             Tuple2.of("a", 2),
                                                                             Tuple2.of("c", 3),
                                                                             Tuple2.of("c", 4),
                                                                             Tuple2.of("d", 5),
                                                                             Tuple2.of("d", 6)
                                                                     )
                                                                     .assignTimestampsAndWatermarks(WatermarkStrategy
                                                                             .<Tuple2<String, Integer>>forMonotonousTimestamps()
                                                                             .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Integer>>() {
                                                                                 @Override
                                                                                 public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
                                                                                     return element.f1 * 1000;
                                                                                 }
                                                                             }));
        
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromElements(
                                                                                      Tuple3.of("a", 1, 1),
                                                                                      Tuple3.of("a", 12, 1),
                                                                                      Tuple3.of("c", 3, 1),
                                                                                      Tuple3.of("c", 14, 1),
                                                                                      Tuple3.of("d", 5, 1),
                                                                                      Tuple3.of("d", 16, 1)
                                                                              )
                                                                              .assignTimestampsAndWatermarks(WatermarkStrategy
                                                                                      .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                                                                                      .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Integer>>() {
                                                                                          @Override
                                                                                          public long extractTimestamp(Tuple3<String, Integer, Integer> element, long recordTimestamp) {
                                                                                              return element.f1 * 1000;
                                                                                          }
                                                                                      }));
        
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(t -> t.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(t -> t.f0);
        
        SingleOutputStreamOperator<String> join = ks1.intervalJoin(ks2)
                                                     .between(Time.seconds(-2), Time.seconds(2))
                                                     .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                                                         @Override
                                                         public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                                                             out.collect(left + " <----> " + right);
                                                         }
                                                     });
        
        join.print();
        // (a,1) <----> (a,1,1)
        // (a,2) <----> (a,1,1)
        // (c,3) <----> (c,3,1)
        // (c,4) <----> (c,3,1)
        // (d,5) <----> (d,5,1)
        // (d,6) <----> (d,5,1)
        
        env.execute();
    }
}
