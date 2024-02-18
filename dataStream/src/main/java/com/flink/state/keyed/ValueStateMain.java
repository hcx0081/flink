package com.flink.state.keyed;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * {@code @description:} 检测每种传感器的水位值，如果连续的两个水位值超过10，输出报警
 */
public class ValueStateMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        SingleOutputStreamOperator<WaterSensor> dataStream = env.socketTextStream("localhost", 8888)
                                                                .map(new MapFunction<String, WaterSensor>() {
                                                                    @Override
                                                                    public WaterSensor map(String value) throws Exception {
                                                                        String[] datas = value.split(",");
                                                                        return new WaterSensor(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
                                                                    }
                                                                })
                                                                /* .assignTimestampsAndWatermarks(WatermarkStrategy
                                                                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                                                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                                                            @Override
                                                                            public long extractTimestamp(WaterSensor waterSensor, long recordTimestamp) {
                                                                                return waterSensor.getVc() * 1000;
                                                                            }
                                                                        })) */;
        
        dataStream.keyBy(waterSensor -> waterSensor.getId())
                  .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                      ValueState<Integer> vcValueState;
                      
                      @Override
                      public void open(Configuration parameters) throws Exception {
                          super.open(parameters);
                          vcValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("vcValueState", Types.INT));
                      }
                      
                      @Override
                      public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                          int lastVc = vcValueState.value() == null ? 0 : vcValueState.value();
                          Integer vc = waterSensor.getVc();
                          if (Math.abs(vc - lastVc) > 10) {
                              out.collect(
                                      "传感器: " + waterSensor.getId() +
                                              ", 当前水位值: " + vc +
                                              ", 上一条水位值: " + lastVc +
                                              " -------- 相差超过10"
                              );
                          }
                          vcValueState.update(vc);
                      }
                  })
                  .print();
        
        env.execute();
    }
}
