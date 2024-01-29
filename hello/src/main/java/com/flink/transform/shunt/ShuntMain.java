package com.flink.transform.shunt;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * {@code @description:}
 */
public class ShuntMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(2);
        
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        
        OutputTag<String> outputTag = new OutputTag<>("slave", Types.STRING);
        SingleOutputStreamOperator<String> process = dataStreamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                int num = Integer.parseInt(value);
                // 奇数（主流）
                if (num % 2 == 1) {
                    out.collect(String.valueOf(num));
                }
                // 偶数（从流）
                else {
                    ctx.output(outputTag, String.valueOf(num));
                }
            }
        });
        
        // 打印主流
        process.print("master");
        // 打印从流
        SideOutputDataStream<String> slave = process.getSideOutput(outputTag);
        slave.print("slave");
        
        env.execute();
    }
}
