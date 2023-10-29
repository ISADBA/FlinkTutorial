package com.atguigu.data_stream_api;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/**
 * ClassName: SinkToFileTest
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/10/29 20:08
 * @Version 1.0
 */
public class SinkToFileTest  {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 2. 从Elements读取数据
        DataStreamSource<Event> eventDataStreamSink = env.fromElements(new Event("Andy", "./home", 1000L),
                        new Event("Mandy", "./cart", 2000L),
                        new Event("Fendy", "./fav", 3000L),
                        new Event("Wendy", "./home", 4000L)
        );

        // 3. 将数据写入文件
        StreamingFileSink<String> output = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(1000L)
                                .withInactivityInterval(1000L)
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build()
                ).build();
        eventDataStreamSink.map(data -> data.toString()).addSink(output);

        // 4. 执行任务
        env.execute();

    }
}
