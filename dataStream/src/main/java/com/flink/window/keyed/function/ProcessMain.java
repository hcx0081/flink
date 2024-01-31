package com.flink.window.keyed.function;

import com.flink.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * {@code @description:}
 */
public class ProcessMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        
        SingleOutputStreamOperator<WaterSensor> dataDS = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
            }
        });
        
        KeyedStream<WaterSensor, String> dataKS = dataDS.keyBy(WaterSensor::getId);
        
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = dataKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        
        SingleOutputStreamOperator<String> process = windowedStream.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String start = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss");
                String end = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss");
                
                long count = elements.spliterator().estimateSize();
                
                out.collect("\n" +
                        "key: " + key + "\n" +
                        "开始时间: " + start + "\n" +
                        "结束时间: " + end + "\n" +
                        "包含 " + count + " 条数据: " + elements);
            }
        });
        
        process.print();
        
        env.execute();
    }
}
