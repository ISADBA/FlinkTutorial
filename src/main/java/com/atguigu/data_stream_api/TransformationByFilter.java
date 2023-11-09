package com.atguigu.data_stream_api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: TransformationByFilter
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/9 08:58
 * @Version 1.0
 */
public class TransformationByFilter {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 2. 从元素读取数据,有界数据集
        DataStreamSource<Event> stream = environment.fromElements(
                new Event("Andy", "./home", 1000L),
                new Event("Mandy", "./cart", 2000L),
                new Event("Fendy", "./fav", 3000L),
                new Event("Wendy", "./home", 4000L)
        );

        // 3. filter转换，内部类
//        stream.filter(new UserExtractor()).print();

        // 3.2 filter转换，lambda表达式
        stream.filter(event -> event.user.equals("Andy")).print();

        // 4. 执行任务
        environment.execute();

    }

    // 自定义FilterFunction,需要实现FilterFunction接口
    public static class UserExtractor implements FilterFunction<Event>{
        @Override
        public boolean filter(Event event) throws Exception {
            if (event.user.equals("Andy")) {
                return true;
            }else {
                return false;
            }
        }
    }
}
