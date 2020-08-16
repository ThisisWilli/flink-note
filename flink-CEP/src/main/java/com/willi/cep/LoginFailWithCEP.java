package com.willi.cep;

import com.willi.bean.LogEvent;
import com.willi.bean.ProcessData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.util.*;

/**
 * \* project: flink-note
 * \* package: com.willi
 * \* author: Willi Wei
 * \* date: 2020-08-16 13:38:50
 * \* description:
 * \TODO: 数据乱序了，双流join时怎么处理
 */
public class LoginFailWithCEP {
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


        // 定义匹配模式
        Pattern<LogEvent, LogEvent> logFailStream = Pattern.<LogEvent>begin("step1").where(new IterativeCondition<LogEvent>() {
            @Override
            public boolean filter(LogEvent value, Context<LogEvent> ctx) throws Exception {
                return value.getContent().startsWith("step1");
            }
        }).followedBy("step2").where(new IterativeCondition<LogEvent>() {
            @Override
            public boolean filter(LogEvent value, Context<LogEvent> ctx) throws Exception {
                return value.getContent().startsWith("step2");
            }
        }).followedBy("step3").where(new IterativeCondition<LogEvent>() {
            @Override
            public boolean filter(LogEvent value, Context<LogEvent> ctx) throws Exception {
                return value.getContent().startsWith("step3");
            }
        }).followedBy("step4").where(new IterativeCondition<LogEvent>() {
            @Override
            public boolean filter(LogEvent value, Context<LogEvent> ctx) throws Exception {
                return value.getContent().startsWith("step4");
            }
        });


        // 将双流进行汇总
        DataStream<LogEvent> unionStream = logStream1.union(logStream2);
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
                        station1 = getRuntimeContext().getListState(new ListStateDescriptor<LogEvent>("station1", LogEvent.class));
                        station2 = getRuntimeContext().getListState(new ListStateDescriptor<LogEvent>("station2", LogEvent.class));
                        station3 = getRuntimeContext().getListState(new ListStateDescriptor<LogEvent>("station3", LogEvent.class));
                        station4 = getRuntimeContext().getListState(new ListStateDescriptor<LogEvent>("station4", LogEvent.class));
                    }


                    @Override
                    public void processElement(LogEvent value, Context ctx, Collector<LogEvent> out) throws Exception {
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

                        if (b1 && b2 && b3 && b4){
//                            System.out.println("b1中的第一条数据为" + station1.get().iterator().next().toString());
//                            System.out.println("b2中的第一条数据为" + station2.get().iterator().next().toString());
//                            System.out.println("b3中的第一条数据为" + station3.get().iterator().next().toString());
//                            System.out.println("b4中的第一条数据为" + station4.get().iterator().next().toString());
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
//        sortedStream.print("sort ");




        // 在事件流上应用模式，得到一个pattern stream
        PatternStream<LogEvent> patternStream = CEP.pattern(sortedStream, logFailStream);

        // 从pattern stream中应用select function 检测出匹配策略
        SingleOutputStreamOperator<ProcessData> result = patternStream.select(new PatternSelectFunction<LogEvent, ProcessData>() {
            @Override
            public ProcessData select(Map<String, List<LogEvent>> map) throws Exception {
                // 从map中按照名称取出对应的事件
                List<LogEvent> s1 = map.get("step1");
                List<LogEvent> s2 = map.get("step2");
                List<LogEvent> s3 = map.get("step3");
                List<LogEvent> s4 = map.get("step4");
                return new ProcessData(
                        new Tuple2<>("step1", s1.iterator().next().getTimeStamp()),
                        new Tuple2<>("step2", s2.iterator().next().getTimeStamp()),
                        new Tuple2<>("step3", s3.iterator().next().getTimeStamp()),
                        new Tuple2<>("step4", s4.iterator().next().getTimeStamp())
                );
            }
        });

        result.print("result => ");
        env.execute();
    }
}