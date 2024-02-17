package com.flink.window.keyed.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * {@code @description:}
 */
public class WindowJoinMain {
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
        
        DataStream<String> join = ds1.join(ds2)
                                     .where(t -> t.f0)
                                     .equalTo(t -> t.f0)
                                     .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                                     .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                                         /**
                                          * @param first The element from first input.
                                          * @param second The element from second input.
                                          * @return
                                          * @throws Exception
                                          */
                                         @Override
                                         public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                                             return first + " <----> " + second;
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
