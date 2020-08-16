package com.willi.cep;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

/**
 * \* project: flink-note
 * \* package: com.willi.cep
 * \* author: Willi Wei
 * \* date: 2020-08-16 14:19:55
 * \* description:
 * \
 */
public class ReadLineSource implements SourceFunction<String> {

    private String filePath;
    private Long sleepTime;
    private boolean canceled = false;

    public ReadLineSource(String filePath, Long sleepTime) {
        this.sleepTime = sleepTime;
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        while (!canceled && reader.ready()){
            String line = reader.readLine();
            ctx.collect(line);
            Thread.sleep(this.sleepTime * 1000);
        }
        ctx.close();
    }

    @Override
    public void cancel() {
        canceled = true;
    }
}