package com.flink.udf;

import com.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * {@code @description:} 自定义标量函数
 */
public class MyScalarFunctionMain {
    public static void main(String[] args) {
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
        
        // 注册UDF
        tEnv.createTemporarySystemFunction("HashFunction", HashFunction.class);
        
        // SQL调用
        tEnv.sqlQuery("SELECT HashFunction(id) FROM water_sensor")
            .execute().print();
        // Table API调用
        tEnv.from("water_sensor").select(call("HashFunction", $("id")))
            .execute().print();
    }
    
    
    public static class HashFunction extends ScalarFunction {
        // 接受任意类型输入，返回 INT 型输出
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.hashCode();
        }
    }
}
