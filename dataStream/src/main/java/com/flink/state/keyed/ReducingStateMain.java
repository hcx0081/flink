package com.flink.state.keyed;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * {@code @description:} 输出每种传感器最大3个水位值
 */
public class ReducingStateMain {
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
                      ReducingState<Integer> vcReducingState;
                      
                      @Override
                      public void open(Configuration parameters) throws Exception {
                          super.open(parameters);
                          vcReducingState = getRuntimeContext().getReducingState(
                                  new ReducingStateDescriptor<>(
                                          "vcReducingState",
                                          new ReduceFunction<Integer>() {
                                              @Override
                                              public Integer reduce(Integer value1, Integer value2) throws Exception {
                                                  return value1 + value2;
                                              }
                                          },
                                          Types.INT
                                  )
                          );
                      }
                      
                      @Override
                      public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                          vcReducingState.add(waterSensor.getVc());
                          
                          out.collect("传感器: " + waterSensor.getId() + ", 水位值总和: " + vcReducingState.get());
                      }
                  })
                  .print();
        
        env.execute();
    }
}
