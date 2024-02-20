package com.flink.state.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:} 在map算子中计算数据的个数
 */
public class ListStateMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        env.socketTextStream("localhost", 8888)
           .map(new MyCountMapFunction())
           /* .assignTimestampsAndWatermarks(WatermarkStrategy
                   .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                   .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                       @Override
                       public long extractTimestamp(WaterSensor waterSensor, long recordTimestamp) {
                           return waterSensor.getVc() * 1000;
                       }
                   })) */
           .print();
        
        env.execute();
    }
    
    public static class MyCountMapFunction implements MapFunction<String, Integer>, CheckpointedFunction {
        private Integer count = 0;
        private ListState<Integer> listState;
        
        @Override
        public Integer map(String value) throws Exception {
            return ++count;
        }
        
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState");
            
            listState.clear();
            listState.add(count);
        }
        
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState");
            
            listState = context.getOperatorStateStore()
                               .getListState(new ListStateDescriptor<>("listState", Types.INT));
            if (context.isRestored()) {
                for (Integer c : listState.get()) {
                    count += c;
                }
            }
        }
    }
}
