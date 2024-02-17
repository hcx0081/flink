package com.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:} DataStream API 实现 WordCount
 */
public class WordCountUnboundedStreamMain {
    // public static void main(String[] args) throws Exception {
    //     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //     DataStreamSource<String> ds = env.socketTextStream("192.168.1.195", 8888);
    //
    //     SingleOutputStreamOperator<Tuple2<String, Integer>> wordOne = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
    //         @Override
    //         public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
    //             String[] words = value.split(" ");
    //             for (String word : words) {
    //                 Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
    //                 collector.collect(wordTuple2);
    //             }
    //         }
    //     });
    //     // 按照word分组
    //     KeyedStream<Tuple2<String, Integer>, String> wordOneKeyBy = wordOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
    //         @Override
    //         public String getKey(Tuple2<String, Integer> value) throws Exception {
    //             return value.f0;
    //         }
    //     });
    //     // 各分组内聚合
    //     SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordOneKeyBy.sum(1);
    //     // 输出
    //     sum.print();
    //     // 执行（必须调用）
    //     env.execute();
    // }
    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds = env.socketTextStream("192.168.1.195", 8888);
        
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOne = ds.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, collector) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                collector.collect(wordTuple2);
            }
        });
        /* 使用lambda表达式需要指定返回类型 */
        wordOne = wordOne.returns(Types.TUPLE(Types.STRING, Types.INT));
        // 按照word分组
        KeyedStream<Tuple2<String, Integer>, String> wordOneKeyBy = wordOne.keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0);
        // 各分组内聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordOneKeyBy.sum(1);
        // 输出
        sum.print();
        // 执行（必须调用）
        env.execute();
    }
}