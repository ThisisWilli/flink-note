package com.willi.hdfs;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class useHdfsConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("topic1"
                , new SimpleStringSchema(), new Properties()));
        StreamingFileSink<Object> sink = StreamingFileSink.forRowFormat(new Path("/out/tmp"), new SimpleStringEncoder<>("UTF-8"))
                /**
                 * 配置桶分配策略
                 * DateTimeBucketAssigner--默认的桶分配政策，默认基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH
                 * BasePathBucketAssigner ：将所有部分文件（part file）存储在基本路径中的分配器（单个全局桶）
                 */
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                /**
                 * 三种滚动策略
                 * 1. CheckpointRollingPolicy
                 * 2. DefaultRollingPolicy
                 * 3. OnCheckpointRollingPolicy
                 */
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        /**
                         * 滚动策略决定了写出文件的状态变化过程
                         * 1. IN-progress 当前文件正在写入
                         * 2. Pending 当处于IN-progress状态的文件close后，就变成Pending状态
                         * 3. Finished 在成功checkpoint后，Pending状态文件变成Finish状态
                         *
                         * 观察到的现象
                         * 1. 会根据本地时间和时区，先创建桶目录
                         * 2. 文件名称规则 part<subtaskIndex><partFileIndex>
                         */
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(2))
                        .withMaxPartSize(10 * 1024 * 1024)
                        .build())
                .build();
    }
}
