package com.willi;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import java.util.List;

/**
 * \* project: flink-note
 * \* package: com.willi
 * \* author: Willi Wei
 * \* date: 2020-08-14 22:02:36
 * \* description:
 * \
 */
public class WatermarkSolveUnorderData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> source = env.readTextFile("data/sensor.txt");
        SingleOutputStreamOperator<SensorReading> stream = source.map(data -> new SensorReading(
                        data.split(",")[0].trim(),
                        Long.parseLong(data.split(",")[1].trim()),
                        Double.parseDouble(data.split(",")[2].trim())
                )
        ).returns(SensorReading.class);


        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimeStamp();
                    }
                })).keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<SensorReading, SensorReading, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) throws Exception {
                        System.out.println("进行窗口处理");
                        ArrayList<SensorReading> list = new ArrayList<>((Collection<? extends SensorReading>) input);
                        list.forEach(out::collect);
                    }
                }).print();
        env.execute();
    }
}