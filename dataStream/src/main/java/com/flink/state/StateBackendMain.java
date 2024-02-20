package com.flink.state;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:}
 */
public class StateBackendMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        env.setStateBackend(new HashMapStateBackend());
        
        env.execute();
    }
}
