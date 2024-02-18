package com.flink.state.keyed;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * {@code @description:} 输出每种传感器每种水位值出现的次数
 */
public class MapStateMain {
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
                      MapState<Integer, Integer> vcMapState;
                      
                      @Override
                      public void open(Configuration parameters) throws Exception {
                          super.open(parameters);
                          vcMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("vcMapState", Types.INT, Types.INT));
                      }
                      
                      @Override
                      public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                          Integer vc = waterSensor.getVc();
                          
                          if (vcMapState.contains(vc)) {
                              vcMapState.put(vc, vcMapState.get(vc) + 1);
                          } else {
                              vcMapState.put(vc, 1);
                          }
                          
                          StringBuilder str = new StringBuilder();
                          for (Map.Entry<Integer, Integer> entry : vcMapState.entries()) {
                              str.append("{ 水位值: " + entry.getKey() + ", 次数: " + entry.getValue() + " } ");
                          }
                          
                          out.collect("传感器: " + waterSensor.getId() + ", 每种水位值出现的次数: " + str);
                      }
                  })
                  .print();
        
        env.execute();
    }
}
