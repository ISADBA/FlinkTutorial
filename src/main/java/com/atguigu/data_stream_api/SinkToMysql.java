package com.atguigu.data_stream_api;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.sql.Timestamp;

/**
 * ClassName: SinkToMysql
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/10/29 20:31
 * @Version 1.0
 */
public class SinkToMysql {
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

        // 3. 将数据写入MySQL
        eventDataStreamSink.addSink(JdbcSink.sink(
                "INSERT INTO `click_events` (`name`, `url`) VALUES (?,?)",
                (ps, t) -> {
                    ps.setString(1, t.user);
                    ps.setString(2, t.url);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://127.0.0.1:22233/test?characterEncoding=utf8&useSSL=false")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("msandbox")
                        .build()
        ));

        // 4. 执行任务
        env.execute();
    }
}
