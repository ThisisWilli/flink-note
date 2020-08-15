package com.willi.watermark;

import com.willi.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * \* project: flink-note
 * \* package: com.willi.watermark
 * \* author: Willi Wei
 * \* date: 2020-08-15 20:07:50
 * \* description:
 * \
 */
public class SolveOutOfOrder {
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

        SingleOutputStreamOperator<SensorReading> result = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimeStamp();
                    }
                })).keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(10))
                .process(new ProcessWindowFunction<SensorReading, SensorReading, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<SensorReading> elements, Collector<SensorReading> out) throws Exception {
                        System.out.println("window : [" + context.window().getStart() + ", " + context.window().getEnd() + ")");
                        ArrayList<SensorReading> list = new ArrayList<SensorReading>((Collection<? extends SensorReading>) elements);
                        list.sort((o1, o2) -> (int) (o1.getTimeStamp() - o2.getTimeStamp()));
                        list.forEach(out::collect);
                    }
                });
        result.print();
        env.execute();
    }
}