package com.flink.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;

/**
 * {@code @description:}
 */
public class KafkaMain {
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
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        
        
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                                                     .setBootstrapServers("192.168.100.100:9092")
                                                     .setTopics("test")
                                                     .setValueOnlyDeserializer(new SimpleStringSchema())
                                                     .setStartingOffsets(OffsetsInitializer.latest())
                                                     .build();
        DataStreamSource<String> dataStreamSource = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)),
                "kafkaSource"
        );
        
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                                               .setBootstrapServers("192.168.100.100:9092")
                                               .setRecordSerializer(
                                                       KafkaRecordSerializationSchema.<String>builder()
                                                                                     .setTopic("ws")
                                                                                     .setValueSerializationSchema(new SimpleStringSchema())
                                                                                     .build()
                                               )
                                               .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                               .setTransactionalIdPrefix("tra-")
                                               .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                                               .build();
        dataStreamSource.sinkTo(kafkaSink);
        
        
        env.execute();
    }
}
