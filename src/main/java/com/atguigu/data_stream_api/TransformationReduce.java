package com.atguigu.data_stream_api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: TransformationReduce
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/10 09:20
 * @Version 1.0
 */
public class TransformationReduce {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(6);

        // 2. 从元素读取数据,有界数据集
        DataStreamSource<Event> stream = environment.fromElements(
                new Event("Andy", "./home", 1000L),
                new Event("Mandy", "./cart", 2000L),
                new Event("Fendy", "./fav", 3000L),
                new Event("Andy", "./home", 4000L),
                new Event("Andy", "./home", 1000L),
                new Event("Andy", "./home", 1000L),
                new Event("Tndy", "./home", 4000L),
                new Event("Wendy", "./home", 4000L),
                new Event("Wendy", "./home", 4000L),
                new Event("Wendy", "./home", 4000L),
                new Event("Wendy", "./home", 4000L),
                new Event("Wendy", "./home", 4000L),
                new Event("Wendy", "./home", 4000L),
                new Event("Wendy", "./home", 4000L)
        );

        // 3. 将stream转换为元组(用户名，pv的格式)
        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        });

        // 4. 接续处理stream2，使用用户名进行分流，然后每到一条用户，pv就加1
        // value1 代表上一次的累加结果，value2代表当前的数据
        SingleOutputStreamOperator<Tuple2<String, Long>> stream3 = stream2.keyBy(event -> event.f0).reduce((value1, value2) -> {
            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
        });
//        stream3.print();

        // 5. 将累加器更新为当前最大的pv统计值
        stream3.keyBy(event -> true).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                System.out.println("value1: " + value1);
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        }).print();

        // 6. 执行任务
        environment.execute();

    }
}
