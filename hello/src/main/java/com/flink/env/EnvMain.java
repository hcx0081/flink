package com.flink.env;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code @description:}
 */
public class EnvMain {
    public static void main(String[] args) {
        StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
