package com.willi.watermark;

import com.willi.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

/**
 * \* project: flink-note
 * \* package: com.willi.watermark
 * \* author: Willi Wei
 * \* date: 2020-08-15 18:46:42
 * \* description:
 * \
 */
public class UseforMonotonous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 默认是200ms
        env.getConfig().setAutoWatermarkInterval(1L);
        DataStreamSource<String> source = env.socketTextStream("192.168.177.211", 7777);
        SingleOutputStreamOperator<SensorReading> stream = source.map(data -> new SensorReading(
                        data.split(",")[0].trim(),
                        Long.parseLong(data.split(",")[1].trim()),
                        Double.parseDouble(data.split(",")[2].trim())
                )
        ).returns(SensorReading.class);

        // 间歇性生成 watermark， 设置最长容忍乱序数据时间为6
        SingleOutputStreamOperator<SensorReading> result = stream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp()))
                .keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<SensorReading, SensorReading, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) throws Exception {
                        System.out.println("window : [" + window.getStart() + ", " + window.getEnd() + "]");
                        ArrayList<SensorReading> list = new ArrayList<>((Collection<? extends SensorReading>) input);
                        list.forEach(out::collect);
                    }
                });
        result.print();
        env.execute();
    }
}