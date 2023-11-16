package com.atguigu.data_stream_api;

import com.mysql.cj.jdbc.JdbcPropertySet;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: DemoMysqlCdc
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/14 22:06
 * @Version 1.0
 */


public class DemoMysqlCdc {
    public static void main(String[] args) {
        try {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(22233)
                .databaseList("test") // set captured database
                .tableList("test.t") // set captured table
                .username("root")
                .password("msandbox")
                .serverTimeZone("Asia/Shanghai")
                .serverId("5000-6000")
                .deserializer(new JsonDebeziumDeserializationSchema(true)) // converts SourceRecord to JSON String
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // enable checkpoint
        env.enableCheckpointing(3000);
        // 设置checkpoint的路径
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/checkpoint");

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .print();
        env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
