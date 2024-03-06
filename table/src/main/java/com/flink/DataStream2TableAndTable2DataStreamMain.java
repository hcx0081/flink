package com.flink;

import com.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * {@code @description:}
 */
public class DataStream2TableAndTable2DataStreamMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStreamSource<WaterSensor> dataStream = env.fromElements(
                new WaterSensor("1", 1L, 1),
                new WaterSensor("2", 2L, 2),
                new WaterSensor("3", 3L, 3)
        );
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 流转表
        Table waterSensorTable = tEnv.fromDataStream(dataStream);
        tEnv.createTemporaryView("water_sensor", waterSensorTable);
        Table table = tEnv.sqlQuery("select * from water_sensor");
        table.execute().print();
        
        // 表转流
        // DataStream<WaterSensor> waterSensorDataStream = tEnv.toDataStream(table, WaterSensor.class);
        // waterSensorDataStream.print();
        // env.execute();
    }
}
