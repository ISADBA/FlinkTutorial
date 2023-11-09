package com.atguigu.data_stream_api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: TransformationByFlatMap
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/9 09:06
 * @Version 1.0
 */
public class TransformationByFlatMap {
    public static void main(String[] args) throws Exception{
        // 1. 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 从元素读取数据,有界数据集
        DataStreamSource<Event> stream = environment.fromElements(
                new Event("Andy", "./home", 1000L),
                new Event("Mandy", "./cart", 2000L),
                new Event("Fendy", "./fav", 3000L),
                new Event("Wendy", "./home", 4000L)
        );

        // 3. flatMap转换，内部类
        stream.flatMap(new MyFlatMap()).print();

        // 4. 执行任务
        environment.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event,String>{
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            // 对Andy进行map操作，对Mandy进行filter操作
            if (event.user.equals("Andy")) {
                collector.collect("BigAndy");
            } else if (!event.user.equals("Mandy") ){
                collector.collect(event.user);
            }
        }
    }
}
