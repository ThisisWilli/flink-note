package com.willi.bean;

/**
 * \* project: flink-note
 * \* package: com.willi.bean
 * \* author: Willi Wei
 * \* date: 2020-08-16 14:20:23
 * \* description:
 * \
 */
public class LogEvent {
    private String id;
    private String workpiece;
    private Long timeStamp;
    private String content;

    public LogEvent(String id, String workpiece, Long timeStamp, String content) {
        this.id = id;
        this.workpiece = workpiece;
        this.timeStamp = timeStamp;
        this.content = content;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getWorkpiece() {
        return workpiece;
    }

    public void setWorkpiece(String workpiece) {
        this.workpiece = workpiece;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "LogEvent{" +
                "id='" + id + '\'' +
                ", workpiece='" + workpiece + '\'' +
                ", timeStamp=" + timeStamp +
                ", content='" + content + '\'' +
                '}';
    }
}