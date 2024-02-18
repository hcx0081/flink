package com.flink.process;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * {@code @description:}
 */
public class OutputMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        SingleOutputStreamOperator<WaterSensor> dataStream = env.socketTextStream("localhost", 8888)
                                                                .map(new MapFunction<String, WaterSensor>() {
                                                                    @Override
                                                                    public WaterSensor map(String value) throws Exception {
                                                                        String[] datas = value.split(",");
                                                                        return new WaterSensor(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
                                                                    }
                                                                });
        
        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        SingleOutputStreamOperator<String> process = dataStream.process(new ProcessFunction<WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                // 从流
                if (waterSensor.getVc() > 10) {
                    ctx.output(warnTag, waterSensor + " 当前水位: " + waterSensor.getVc() + ", 大于阈值10");
                }
                // 主流
                out.collect(waterSensor.toString());
            }
        });
        
        process.getSideOutput(warnTag).printToErr("从流");
        process.print("主流");
        
        env.execute();
    }
}
