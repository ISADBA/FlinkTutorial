package com.atguigu.data_stream_api;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: TransformationKeyBy
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/9 09:24
 * @Version 1.0
 */
public class TransformationKeyBy {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 2. 从元素读取数据,有界数据集
        DataStreamSource<Event> stream = environment.fromElements(
                new Event("Andy", "./home", 1000L),
                new Event("Mandy", "./cart", 2000L),
                new Event("Fendy", "./fav", 3000L),
                new Event("Andy", "./home", 4000L)
        );

        // 3. keyBy转换，内部类
//        KeyedStream<Event, String> keyedStream = stream.keyBy(new MyKeyBy());

        // 3.2 keyBy转换，lambda表达式
        KeyedStream<Event, String> keyedStream = stream.keyBy(event -> event.user);

        // 4. 打印
        keyedStream.print();

        // 5. 执行任务
        environment.execute();
    }

    public static class MyKeyBy implements KeySelector<Event,String> {
        @Override
        public String getKey(Event event) throws Exception {
            return event.user;
        }
    }
}
