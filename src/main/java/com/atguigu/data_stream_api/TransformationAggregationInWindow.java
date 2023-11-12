package com.atguigu.data_stream_api;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * ClassName: TransformationReduce
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/10 09:20
 * @Version 1.0
 */
public class TransformationAggregationInWindow {
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
        // Andy,/home,1699340702

        // 3. 对将数据转换为Event对象
        DataStream<Event> streamEvent = stream.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Event(fields[0], fields[1], Long.parseLong(fields[2]));
            }
        });
//        streamEvent.print();

        // 4. 使用内置的forBoundedOutOfOrderness无序的watermark生成时间水位
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

        // 5. 使用滚动事件时间窗口+AggregateFunction进行聚合
        streamWatermarks.keyBy(event -> event.user)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AvgPv())
                .print();

        // 6. 执行任务
        environment.execute();

    }

    // 实现一个AggregateFunction
    public static class AvgPv implements AggregateFunction<Event,Tuple2<HashSet<String>, Long>, Double>{
        // 创建累加器
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            return Tuple2.of(new HashSet<>(),0L);
        }

        // 属于本窗口的数据来一条累加一次，并返回累加器
        @Override
        public Tuple2<HashSet<String>, Long> add(Event event, Tuple2<HashSet<String>, Long> acc) {
            acc.f0.add(event.user);
            acc.f1 += 1;
            return acc;
        }

        // 窗口闭合时，增量聚合结束，将计算结果发送到下游
        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> acc) {
            return (double)  acc.f1/ acc.f0.size();
        }

        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> acc1, Tuple2<HashSet<String>, Long> acc2) {
            return null;
        }
    }
}