package com.willi.cep;

import com.willi.bean.LogEvent;
import com.willi.bean.ProcessData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * \* project: flink-note
 * \* package: com.willi.cep
 * \* author: Willi Wei
 * \* date: 2020-08-18 16:24:37
 * \* description:
 * \
 */
public class TestTTL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 工位1的数据
        DataStreamSource<String> source1 = env.addSource(new ReadLineSource("data/logEventSource1.txt", 1L));
        SingleOutputStreamOperator<LogEvent> logStream1
                = source1.map(line -> new LogEvent(
                line.split(",")[0].trim(),
                line.split(",")[1].trim(),
                Long.parseLong(line.split(",")[2].trim()),
                line.split(",")[3].trim())
        ).assignTimestampsAndWatermarks(WatermarkStrategy
                .<LogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp()));


        // 工位2中的数据
        DataStreamSource<String> source2 = env.addSource(new ReadLineSource("data/logEventSource2.txt", 2L));
        SingleOutputStreamOperator<LogEvent> logStream2 = source2.map(line -> new LogEvent(
                line.split(",")[0].trim(),
                line.split(",")[1].trim(),
                Long.parseLong(line.split(",")[2].trim()),
                line.split(",")[3].trim())
        ).assignTimestampsAndWatermarks(WatermarkStrategy
                .<LogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp()));

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        // 将双流进行汇总
        DataStream<LogEvent> unionStream = logStream1;
//        unionStream.print("union ");
        SingleOutputStreamOperator<LogEvent> sortedStream = unionStream
                .keyBy(LogEvent::getWorkpiece)
                .process(new KeyedProcessFunction<String, LogEvent, LogEvent>() {
                    ListState<LogEvent> station1;
                    ListState<LogEvent> station2;
                    ListState<LogEvent> station3;
                    ListState<LogEvent> station4;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 先创建ListState成员变量，再创建stateDescriptor
                        ListStateDescriptor<LogEvent> d1 = new ListStateDescriptor<>("station1", LogEvent.class);
                        d1.enableTimeToLive(ttlConfig);
                        station1 = getRuntimeContext().getListState(d1);

                        ListStateDescriptor<LogEvent> d2 = new ListStateDescriptor<>("station2", LogEvent.class);
                        d2.enableTimeToLive(ttlConfig);
                        station2 = getRuntimeContext().getListState(d2);

                        ListStateDescriptor<LogEvent> d3 = new ListStateDescriptor<>("station3", LogEvent.class);
                        d3.enableTimeToLive(ttlConfig);
                        station3 = getRuntimeContext().getListState(d3);

                        ListStateDescriptor<LogEvent> d4 = new ListStateDescriptor<>("station4", LogEvent.class);
                        d4.enableTimeToLive(ttlConfig);
                        station4 = getRuntimeContext().getListState(d4);
                    }


                    @Override
                    public void processElement(LogEvent value, Context ctx, Collector<LogEvent> out) throws Exception {
                        System.out.println(value);

                        if (value.getContent().contains("step1")) {
                            station1.add(value);
                        }
                        if (value.getContent().contains("step2")) {
                            station2.add(value);
                        }
                        if (value.getContent().contains("step3")) {
                            station3.add(value);
                        }
                        if (value.getContent().contains("step4")) {
                            station4.add(value);
                        }

                        boolean b1 = station1.get().iterator().hasNext();
                        boolean b2 = station2.get().iterator().hasNext();
                        boolean b3 = station3.get().iterator().hasNext();
                        boolean b4 = station4.get().iterator().hasNext();
                        System.out.println(getLength(station1));
                        System.out.println(getLength(station2));
                        System.out.println(getLength(station3));
                        System.out.println(getLength(station4));

                        if (b1 && b2 && b3 && b4){
                            out.collect(station1.get().iterator().next());
                            removeFirstState(station1);
                            out.collect(station2.get().iterator().next());
                            removeFirstState(station2);
                            out.collect(station3.get().iterator().next());
                            removeFirstState(station3);
                            out.collect(station4.get().iterator().next());
                            removeFirstState(station4);
                        }
                    }

                    public int getLength(ListState<LogEvent> listState){
                        int size = 0;
                        try {
                            Iterator<LogEvent> iterator = listState.get().iterator();
                            while (iterator.hasNext()){
                                size++;
                                iterator.next();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return size;
                    }
                    public void removeFirstState(ListState<LogEvent> stateList){
                        try {
                            LinkedList<LogEvent> temp = new LinkedList<>();
                            for (LogEvent logEvent : stateList.get()) {
                                temp.addLast(logEvent);
                            }
                            temp.removeFirst();
                            stateList.clear();
                            stateList.addAll(temp);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
        env.execute();
    }
}