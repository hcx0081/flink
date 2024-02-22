package com.flink.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * {@code @description:}
 */
public class KafkaSourceMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                                                     .setBootstrapServers("192.168.100.100:9092")
                                                     .setTopics("ws")
                                                     .setValueOnlyDeserializer(new SimpleStringSchema())
                                                     .setStartingOffsets(OffsetsInitializer.latest())
                                                     .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                                                     .build();
        DataStreamSource<String> dataStreamSource = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafkaSource"
        );
        
        dataStreamSource.print();
        
        env.execute();
    }
}
