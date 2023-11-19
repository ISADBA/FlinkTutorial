package com.atguigu.data_stream_api;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
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
        // --hostname 10.130.34.101 --port 3306 --username andy --password 123456 --databaseList test --tableList test.t,test.t1 --setupMode initial
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        String username = parameterTool.get("username");
        String password = parameterTool.get("password");
        String databaseList = parameterTool.get("databaseList");
        String tableList = parameterTool.get("tableList");
        String setupMode = parameterTool.get("setupMode");
        StartupOptions mode = StartupOptions.initial();
        if (setupMode.equals("latest")) {
            mode = StartupOptions.latest();
        }
        try {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .scanNewlyAddedTableEnabled(true)
                .databaseList(databaseList) // set captured database example "test"
                .tableList(tableList) // set captured table example: "test.t,test.t1"
                .username(username)
                .password(password)
                .serverTimeZone("Asia/Shanghai")
                .serverId("5000-6000")
                .startupOptions(mode)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
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
