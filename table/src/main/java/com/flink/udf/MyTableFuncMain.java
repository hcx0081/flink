package com.flink.udf;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * {@code @description:} 自定义表值函数
 */
public class MyTableFuncMain {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStreamSource<String> dataStream = env.fromElements(
                "hello world",
                "hello java",
                "hello flink"
        );
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 流转表
        Table table = tEnv.fromDataStream(dataStream, Schema.newBuilder()
                                                            .column("words", DataTypes.STRING())
                                                            .build());
        tEnv.createTemporaryView("table", table);
        
        // 注册UDF
        tEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        
        // SQL调用
        tEnv
                // .sqlQuery("SELECT words, word, length FROM table, LATERAL TABLE(SplitFunction(words));")
                // .sqlQuery("SELECT words, word, length FROM table left join LATERAL TABLE(SplitFunction(words)) on true;")
                .sqlQuery("SELECT words, newWord, newLength FROM table left join LATERAL TABLE(SplitFunction(words)) as T(newWord, newLength) on true;")
                .execute().print();
        // Table API调用
        // tEnv.from("table").select(call("SplitFunction", $("words")))
        //     .execute().print();
    }
    
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {
        public void eval(String str) {
            for (String s : str.split(" ")) {
                collect(Row.of(s, s.length()));
            }
        }
    }
}
