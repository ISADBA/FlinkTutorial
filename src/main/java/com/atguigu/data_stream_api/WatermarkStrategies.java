package com.atguigu.data_stream_api;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * ClassName: TransformationByMap
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/9 08:41
 * @Version 1.0
 */
public class WatermarkStrategies {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // 设置水位线间隔为1000ms
        environment.getConfig().setAutoWatermarkInterval(1000);
        // 2. 从元素读取数据,有界数据集
        DataStreamSource<Event> stream = environment.fromElements(
                new Event("Andy", "./home", 1000L),
                new Event("Mandy", "./cart", 2000L),
                new Event("Fendy", "./fav", 3000L),
                new Event("Wendy", "./home", 4000L)
        );

        // 3. 设置水位线策略
        // 使用内置的forMonotonousTimestamps有序流的watermark生成
        DataStream<Event> streamWatermarks1 = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<Event>(){
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp ;
                    }
                }
        ));

        // 使用内置的forBoundedOutOfOrderness无序的watermark生成时间水位
        DataStream<Event> streamWatermarks2 = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(100)).withTimestampAssigner(
                new SerializableTimestampAssigner<Event>(){
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp ;
                    }
                }
        ));


        // 3.2 map转换，lambda表达式
        streamWatermarks1.map(event -> event.user).print();
//        stream.map(event -> "http://" + event.url ).print();
        // 4. 执行任务
        environment.execute();


    }
    // 自定义MapFunction,需要实现MapFunction接口
    public static class UserExtractor implements MapFunction<Event,String>{
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}

