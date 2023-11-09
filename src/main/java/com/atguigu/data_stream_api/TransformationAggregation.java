package com.atguigu.data_stream_api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: TransformationAggregation
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/9 09:39
 * @Version 1.0
 */
public class TransformationAggregation {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 2. 从元素读取数据,有界数据集
        DataStreamSource<Tuple2> stream = environment.fromElements(
                Tuple2.of("Andy", 1000L),
                Tuple2.of("Mandy", 2000L),
                Tuple2.of("Fendy", 3000L),
                Tuple2.of("Andy", 4000L)
        );

        // 3. 对数据进行keyby然后进行聚合
//        stream.keyBy(event -> event.f0).sum(1).print();
//        stream.keyBy(event -> event.f0).min("f1").print();
//        stream.keyBy(event -> event.f0).sum("f1").print();
        // 统计stream中f1的和
        SingleOutputStreamOperator<Long> sum = stream.map(event -> (Long) event.f1).keyBy(event -> "sum").sum(0);
        sum.print();

        // 4. 执行任务
        environment.execute();
    }
}
