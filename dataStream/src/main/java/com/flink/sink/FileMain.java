package com.flink.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * {@code @description:}
 */
public class FileMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(2);
        
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "数据生成：" + value.toString();
                    }
                },
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );
        
        DataStreamSource<String> dataStreamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGen");
        
        dataStreamSource.sinkTo(FileSink.forRowFormat(new Path("data/sink"), new SimpleStringEncoder<String>())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("sink")
                        .build())
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withInactivityInterval(Duration.ofSeconds(10))
                        .withMaxPartSize(new MemorySize(1024 * 1024))
                        .build())
                .build());
        
        env.execute();
    }
}
