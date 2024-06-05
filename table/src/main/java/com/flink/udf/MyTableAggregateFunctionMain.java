package com.flink.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * {@code @description:}
 */
public class MyTableAggregateFunctionMain {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStreamSource<Integer> dataStream = env.fromElements(3, 6, 12, 5, 8, 9, 4);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 流转表
        Table dataTable = tEnv.fromDataStream(dataStream, Schema.newBuilder()
                                                                .column("f0", DataTypes.INT())
                                                                .build())
                              .as("data");
        tEnv.createTemporaryView("number", dataTable);
        
        // 注册UDF
        tEnv.createTemporarySystemFunction("Top2Function", Top2Function.class);
        
        dataTable.flatAggregate(call("Top2Function", $("data")).as("value", "rank"))
                 .select($("value"), $("rank"))
                 .execute().print();
    }
    
    // TableAggregateFunction<输出类型, Tuple2<数据, 排名>>
    public static class Top2Function extends TableAggregateFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(-1, -1);
        }
        
        public void accumulate(Tuple2<Integer, Integer> acc, Integer data) {
            if (data > acc.f0) {
                acc.f1 = acc.f0;
                acc.f0 = data;
            } else if (data > acc.f1) {
                acc.f1 = data;
            }
        }
        
        public void emitValue(Tuple2<Integer, Integer> acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.f0 != -1) {
                out.collect(Tuple2.of(acc.f0, 1));
            }
            if (acc.f1 != -1) {
                out.collect(Tuple2.of(acc.f1, 2));
            }
        }
    }
}
