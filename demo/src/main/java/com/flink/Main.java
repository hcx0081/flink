package com.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        tEnv.executeSql("CREATE TABLE StreamSourceTable(content STRING) WITH(\n" +
                "  'connector' = 'datagen',\n" +
                "  'fields.content.kind' = 'sequence',\n" +
                "  'fields.content.start' = '1',\n" +
                "  'fields.content.end' = '1000'\n" +
                ");");
        
        tEnv.executeSql("CREATE TABLE StreamSinkTable (content STRING) WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "  'url' = 'jdbc:mysql://192.168.100.100:3306/test_flink',\n" +
                "  'table-name' = 'stream_sink_table',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '200081'\n" +
                ");");
        
        tEnv.executeSql("INSERT INTO\n" +
                "  StreamSinkTable\n" +
                "SELECT\n" +
                "  content\n" +
                "FROM\n" +
                "  StreamSourceTable;");
    }
}