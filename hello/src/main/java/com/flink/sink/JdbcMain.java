package com.flink.sink;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * {@code @description:}
 */
public class JdbcMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(2);
        
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        
        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
            }
        });
        
        map.addSink(JdbcSink.sink(
                "insert into ws value(?, ?, ?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder()
                                    .withMaxRetries(3)
                                    .withBatchSize(100)
                                    .withBatchIntervalMs(3000)
                                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://demo3.datagoo.cn:3306/test")
                        .withUsername("root")
                        .withPassword("datagoo")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        ));
        
        env.execute();
    }
}
