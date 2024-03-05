package com.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * {@code @description:}
 */
public class TableMain {
    public static void main(String[] args) {
        // EnvironmentSettings settings = EnvironmentSettings.newInstance()
        //                                                   .inStreamingMode()
        //                                                   .build();
        // TableEnvironment tEnv = TableEnvironment.create(settings);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        tEnv.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");");
        
        tEnv.executeSql("CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ");");
        
        // Table table1 = tEnv.sqlQuery("select * from source where id > 5;");
        // tEnv.createTemporaryView("tmp", table1);
        // tEnv.executeSql("select * from tmp;");
        // tEnv.executeSql("insert into sink select * from tmp;");
        
        Table table2 = tEnv.from("source").where($("id").isGreater(5)).select($("*"));
        tEnv.createTemporaryView("tmp", table2);
        tEnv.from("tmp").select($("*")).executeInsert("sink");
    }
}