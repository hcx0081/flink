package com.flink.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * {@code @description:} 自定义聚合函数
 */
public class MyAggregateFunctionMain {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Tuple2<分数, 权重>
        DataStreamSource<Tuple2<Double, Integer>> dataStream = env.fromElements(
                Tuple2.of(80.0, 1),
                Tuple2.of(90.0, 2),
                Tuple2.of(100.0, 3)
        );
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 流转表
        Table scoreTable = tEnv.fromDataStream(dataStream, Schema.newBuilder()
                                                                 .column("f0", DataTypes.DOUBLE())
                                                                 .column("f1", DataTypes.INT())
                                                                 .build())
                               .as("score", "weight");
        tEnv.createTemporaryView("score", scoreTable);
        
        // 注册UDF
        tEnv.createTemporarySystemFunction("WeightAvgFunction", WeightAvgFunction.class);
        
        // SQL调用
        tEnv.sqlQuery("SELECT WeightAvgFunction(score, weight) as `avg` FROM score")
            .execute().print();
        // Table API调用
        // tEnv.from("score").select(call("WeightAvgFunction", $("score"), $("weight"))).as("avg")
        //     .execute().print();
    }
    
    // AggregateFunction<输出类型, Tuple2<加权总和, 权重总和>>
    public static class WeightAvgFunction extends AggregateFunction<Double, Tuple2<Double, Double>> {
        @Override
        public Double getValue(Tuple2<Double, Double> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }
        
        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return Tuple2.of(0.0, 0.0);
        }
        
        public void accumulate(Tuple2<Double, Double> acc, Double score, Integer weight) {
            acc.f0 += score * weight;
            acc.f1 += weight;
        }
    }
}
