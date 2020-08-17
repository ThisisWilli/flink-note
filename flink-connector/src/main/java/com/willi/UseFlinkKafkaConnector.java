package com.willi;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * \* project: flink-note
 * \* package: com.willi
 * \* author: Willi Wei
 * \* date: 2020-08-17 09:11:57
 * \* description:
 * \
 */
public class UseFlinkKafkaConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("topic1", new SimpleStringSchema(), new Properties()));
    }
}