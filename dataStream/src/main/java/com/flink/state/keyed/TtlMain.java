package com.flink.state.keyed;

import com.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * {@code @description:} 检测每种传感器的水位值，如果连续的两个水位值超过10，输出报警
 */
public class TtlMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        SingleOutputStreamOperator<WaterSensor> dataStream = env.socketTextStream("localhost", 7777)
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
                          
                          ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>(
                                  "vcValueState",
                                  Types.INT
                          );
                          valueStateDescriptor.enableTimeToLive(
                                  StateTtlConfig
                                          .newBuilder(Time.seconds(5))
                                          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                          .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                          .build()
                          );
                          vcValueState = getRuntimeContext().getState(valueStateDescriptor);
                      }
                      
                      @Override
                      public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                          Integer lastVc = vcValueState.value();
                          Integer vc = waterSensor.getVc();
                          out.collect(
                                  "传感器: " + waterSensor.getId() +
                                          ", 当前水位值: " + vc +
                                          ", 上一条水位值: " + lastVc
                          );
                          vcValueState.update(waterSensor.getVc());
                      }
                  })
                  .print();
        
        env.execute();
    }
}
