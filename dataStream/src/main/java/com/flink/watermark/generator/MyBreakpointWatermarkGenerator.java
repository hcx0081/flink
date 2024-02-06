package com.flink.watermark.generator;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * {@code @description:}
 */
public class MyBreakpointWatermarkGenerator<T> implements WatermarkGenerator<T> {
    private long maxTimestamp;
    private long delayTimestamp;
    
    public MyBreakpointWatermarkGenerator(long delayTimestamp) {
        this.maxTimestamp = Long.MIN_VALUE + this.delayTimestamp + 1;
        this.delayTimestamp = delayTimestamp;
    }
    
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        this.maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        output.emitWatermark(new Watermark(this.maxTimestamp - this.delayTimestamp - 1));
        System.out.println("onEvent: " + this.maxTimestamp);
    }
    
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
    }
}
