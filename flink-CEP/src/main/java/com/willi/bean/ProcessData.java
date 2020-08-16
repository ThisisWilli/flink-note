package com.willi.bean;

import scala.Tuple2;

/**
 * \* project: flink-note
 * \* package: com.willi.bean
 * \* author: Willi Wei
 * \* date: 2020-08-16 15:10:01
 * \* description:
 * \
 */
public class ProcessData {
    private Tuple2<String, Long> step1;
    private Tuple2<String, Long> step2;
    private Tuple2<String, Long> step3;
    private Tuple2<String, Long> step4;

    public ProcessData(Tuple2<String, Long> step1, Tuple2<String, Long> step2, Tuple2<String, Long> step3, Tuple2<String, Long> step4) {
        this.step1 = step1;
        this.step2 = step2;
        this.step3 = step3;
        this.step4 = step4;
    }

    @Override
    public String toString() {
        return "ProcessData{" +
                "step1=" + step1 +
                ", step2=" + step2 +
                ", step3=" + step3 +
                ", step4=" + step4 +
                '}';
    }
}