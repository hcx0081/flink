package com.flink.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * {@code @description:}
 */
public class CheckpointMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        
        env.setDefaultSavepointDirectory("");
        
        // 启用检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        // 指定检查点的存储位置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop1:8020/cp");
        checkpointConfig.setCheckpointTimeout(50000);
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        
        checkpointConfig.enableUnalignedCheckpoints();
        
        env.socketTextStream("localhost", 8888)
           .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
               String[] words = value.split(" ");
               for (String word : words) {
                   out.collect(Tuple2.of(word, 1));
               }
           })
           .returns(Types.TUPLE(Types.STRING, Types.INT))
           .keyBy(value -> value.f0);
        
        
        env.execute();
    }
}
