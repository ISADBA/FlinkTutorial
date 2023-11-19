package com.atguigu.table_api;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.atguigu.data_stream_api.Event;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DemoTable
 * Package: com.atguigu.table_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/19 10:12
 * @Version 1.0
 */
public class DemoTable {
    public static void main(String[] args) {
        // 1. 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 2. 从元素读取数据,有界数据集
        DataStreamSource<Event> stream = environment.fromElements(
                new Event("Andy", "./home", 1000L),
                new Event("Mandy", "./cart", 2000L),
                new Event("Fendy", "./fav", 3000L),
                new Event("Andy", "./home", 4000L)
        );

        // 3. 获取表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 4. 将数据流转换成表
        tableEnvironment.fromDataStream(stream);

        // 5. 使用SQL进行查询
        Table table = tableEnvironment.sqlQuery("select * from " + tableEnvironment.fromDataStream(stream) + " where user = 'Andy'");

        // 6. 将表转换成数据流进行输出
        tableEnvironment.toDataStream(table).print();

        // 7. 执行程序
        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
