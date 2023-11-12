package com.atguigu.data_stream_api;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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
public class TransformationWatermarkTest {
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

        // 4. 使用内置的forBoundedOutOfOrderness无序的watermark生成时间水位
        DataStream<Event> streamWatermarks = streamEvent.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
                new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
//                        System.out.println("Timestamp: " + event.timestamp);
                        return event.timestamp;
                    }
                }
        ));
//        streamWatermarks.print();

        // 5. 使用滚动事件时间窗口+AggregateFunction进行聚合
        streamWatermarks.keyBy(event -> event.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new WatermarkTestResult())
                .print();

        // 6. 执行任务
        environment.execute();

    }

    // 自定义处理窗口函数，输出当前的水位线和窗口信息
    public static class WatermarkTestResult extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            // 获取当前的水位线
            long watermark = context.currentWatermark();
            // 获取当前的窗口信息
            TimeWindow window = context.window();
            // 获取当前窗口中的数据
            HashSet<String> users = new HashSet<>();
            for (Event element : elements) {
                users.add(element.user);
            }
            // 输出结果
            out.collect("当前水位线为：" + watermark + "，窗口为：" + window + "，用户数为：" + elements.toString());
        }
    }
//    水位时间是根据数据中的时间戳计算的，窗口时间是根据窗口的算法来计算，比如事件时间就是根据数据中的时间戳来计算的，处理时间就是根据系统时间来计算的
//    当前水位线为：1699784973999，窗口为：TimeWindow{start=1699784960000, end=1699784970000}，用户数为：[Event{user='Andy', url='/home', timestamp=1699784965000}]
//    当前水位线为：1699784973999，窗口为：TimeWindow{start=1699784960000, end=1699784970000}，用户数为：[Event{user='Mandy', url='/list', timestamp=1699784968000}]
//    当前水位线为：1699784983999，窗口为：TimeWindow{start=1699784970000, end=1699784980000}，用户数为：[Event{user='Fendy', url='/goods', timestamp=1699784975000}]
//    当前水位线为：1699784993999，窗口为：TimeWindow{start=1699784980000, end=1699784990000}，用户数为：[Event{user='Fendy', url='/home', timestamp=1699784985000}]
}