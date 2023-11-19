package com.atguigu.table_api;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.atguigu.data_stream_api.Event;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

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
    // Datastream与Table API混合使用进行流处理
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

        // 3. 通过DataStream环境创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 4. 将数据流转换成表
        Table tableStream = tableEnvironment.fromDataStream(stream);

        // 5. 通过tableEnv写SQL来完成transformation,输出是一个Table【推荐】
        Table table1 = tableEnvironment.sqlQuery("select * from " + tableEnvironment.fromDataStream(stream) + " where user = 'Andy'");

        // 5.1 基于tableStream来完成transformation,输出是一个Table
        Table table2 = tableStream.select($("user"), $("url")).where($("user").isEqual("Andy"));

        // 5.2 基于tableEnv的executeSql方法来完成transformation,输出是一个TableResult,类似于命令行输出
        TableResult tableResult = tableEnvironment.executeSql("select * from " + tableEnvironment.fromDataStream(stream) + " where user = 'Andy'");

        // 6. 将表转换成数据流进行输出
        tableEnvironment.toDataStream(table1).print();
        tableEnvironment.toDataStream(table2).print();
        tableResult.print();

        // 7. 执行程序
        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
