package com.atguigu.data_stream_api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: TransformationByMap
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/9 08:41
 * @Version 1.0
 */
public class TransformationByMap {
    public static void main(String[] args)  {
       try {
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

           // 3. map转换，内部类
//        stream.map(new UserExtractor()).print();

           // 3.2 map转换，lambda表达式
           stream.map(event -> event.user).print();
//        stream.map(event -> "http://" + event.url ).print();
           // 4. 执行任务
           environment.execute();
       }catch (Exception e){
           e.printStackTrace();
       }
    }
    // 自定义MapFunction,需要实现MapFunction接口
    public static class UserExtractor implements MapFunction<Event,String>{
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}

