package com.flink;

import com.flink.bean.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

/**
 * {@code @description:}
 */
public class DataStream2TableAndTable2DataStreamMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        DataStreamSource<WaterSensor> dataStream = env.fromElements(
                new WaterSensor("1", 1L, 1),
                new WaterSensor("2", 2L, 2),
                new WaterSensor("3", 3L, 3)
        );
        
        // 流转表
        Table waterSensorTable = tEnv.fromDataStream(dataStream);
        tEnv.createTemporaryView("water_sensor", waterSensorTable);
        Table table = tEnv.sqlQuery("select * from water_sensor");
        // table.execute().print();
        
        // 表转流
        // DataStream<WaterSensor> waterSensorDataStream = tEnv.toDataStream(table, WaterSensor.class);
        // waterSensorDataStream.print();
        // env.execute();
        
        
        DataStream<Row> ds = tEnv.toDataStream(table);
        ds.addSink(JdbcSink.sink(
                "insert into water_sensor(*) values(?,?,?)",
                new JdbcStatementBuilder<Row>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Row row) throws SQLException {
                        row = Row.withNames();
                        Set<String> fieldNames1 = row.getFieldNames(false);
                        Set<String> fieldNames = row.getFieldNames(true);
                        System.out.println(fieldNames1);
                        System.out.println(fieldNames);
                        System.out.println(row);
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://10.0.70.225:4000/bigdata")
                        .withUsername("root")
                        .withPassword("tiDB57@0523#")
                        .build()
        ));
        
        
        env.execute();
    }
}
