package com.atguigu.data_stream_api;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * ClassName: TransformationReduce
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/10 09:20
 * @Version 1.0
 */
public class TransformationReduceInWindow {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        environment.setParallelism(1);
        // 设置水位线间隔为1000ms
        environment.getConfig().setAutoWatermarkInterval(1000);

        // 2. 从socket读取无界数据流 nc -lk 7777
        DataStreamSource<String> stream = environment.socketTextStream("localhost", 7777);
        // 格式
        // 姓名,path,timestamp
        // Andy,/home,1699784965000
        // Mandy,/list,1699784968000
        // Fendy,/goods,1699784975000
        // Fendy,/home,1699784985000
        // Mandy,/home,1699784990000
        // Mandy,/home,1699784995000

        // 3. 对将数据转换为Event对象
        DataStream<Event> streamEvent = stream.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Event(fields[0], fields[1], Long.parseLong(fields[2]));
            }
        });
//        streamEvent.print();

        // 3. 使用内置的forBoundedOutOfOrderness无序的watermark生成时间水位
        DataStream<Event> streamWatermarks = streamEvent.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
                new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        System.out.println("Timestamp: " + event.timestamp);
                        return event.timestamp;
                    }
                }
        ));
//        streamWatermarks.print();

        // 4. 将stream转换为元组(用户名，pv的格式)
        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = streamWatermarks.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        });
//        stream2.print();
        // 5. 接续处理stream2，使用用户名进行分流，然后每到一条用户，pv就加1
        // value1 代表上一次的累加结果，value2代表当前的数据
        SingleOutputStreamOperator<Tuple2<String, Long>> stream3 = stream2.keyBy(event -> event.f0)
                // 滚动处理时间窗口，窗口大小为5s
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 滚动事件时间窗口，窗口大小为10s，偏移量为1s,这个用例有点问题，需要时间时间间隔超过10000s才会触发，原因待排查(原因是传入的时间戳是秒级的，需要传入毫秒级的时间戳,然后10s再乘以10，就是10000)
                .window(TumblingEventTimeWindows.of(Time.seconds(10),Time.seconds(1)))
                // 滑动计数窗口,窗口大小为3，步长为1，每个流接受两个消息后，就会使用当前流里面的数据触发一次计算
//                .countWindow(3,2)
                .reduce((value1, value2) -> {
            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
        });

        stream3.print();


        // 6. 执行任务
        environment.execute();

    }
}

