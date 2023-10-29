package com.atguigu.data_stream_api;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * ClassName: SourceTest
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/10/29 11:42
 * @Version 1.0
 */
public class SourceTest {
    public static void main(String[] args) throws Exception{
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件中读取数据,有界数据集
        DataStreamSource<String> stream1 = env.readTextFile("input/click_events.txt");
        stream1.print();

        // 2. 从集合中读取数据,有界数据集
        ArrayList<Object> nums = new ArrayList<>();
        nums.add(1);
        nums.add(2);
        nums.add(3);
        DataStreamSource<Object> numStream = env.fromCollection(nums);
        numStream.print();

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Andy","./home",1000L));
        events.add(new Event("Andy","./cart",2000L));
        events.add(new Event("Andy","./fav",3000L));
        DataStreamSource<Event> eventsStream = env.fromCollection(events);
        eventsStream.print();

        // 3. 从元素读取数据,有界数据集
        DataStreamSource<Event> eventStream2 = env.fromElements(
                new Event("Andy", "./home", 1000L),
                new Event("Andy", "./cart", 2000L),
                new Event("Andy", "./fav", 3000L)
        );
        eventStream2.print();

        // 4. 从socket文本流读取数据,无界数据集
        // nc -lk 7777
        DataStreamSource<String> socketstream2 = env.socketTextStream("localhost", 7777);
        socketstream2.print();

        // 5. 从kafka读取数据,无界数据集
        // 创建kafka生产者 ./bin/kafka-console-producer.sh  --broker-list 10.130.201.12:9092,10.130.201.13:9092,10.130.201.14:9092 --topic flink_clicks
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.130.201.12:9092,10.130.201.13:9092,10.130.201.14:9092");
        properties.setProperty("group.id", "flink_clicks");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaDataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("flink_clicks", new SimpleStringSchema(), properties));
        kafkaDataStreamSource.print();

        // 执行任务
        env.execute();
    }
}
