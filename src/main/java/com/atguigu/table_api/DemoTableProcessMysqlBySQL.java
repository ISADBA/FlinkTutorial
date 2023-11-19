package com.atguigu.table_api;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * ClassName: DemoTable2
 * Package: com.atguigu.table_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/11/19 11:13
 * @Version 1.0
 */
public class DemoTableProcessMysqlBySQL {
    // 纯Table API进行流处理
    public static void main(String[] args) {
        // 1. 通过定义环境配置来创建表环境
        EnvironmentSettings envSettings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        // 2. 通过环境配置来创建表环境
        TableEnvironment tableEnvironment = TableEnvironment.create(envSettings);
        // 3. 创建输入表,从MySQL读取
        String inputTable = "CREATE TABLE t_input (" +
                "`user` STRING," +
                "url STRING," +
                "pv INT" +
                ") WITH (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://10.130.34.101:3306/test'," +
                "'table-name' = 't_click'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'username' = 'andy'," +
                "'password' = '123456')";
        // 3.1 将t_input注册到环境中
        tableEnvironment.executeSql(inputTable);
        // 4. 创建输出表,写入MySQL
        // --hostname 10.130.34.101 --port 3306 --username andy --password 123456
        // jdbc的小版本号最好也对应上
        String outTable = "CREATE TABLE t_output (" +
                "`user` STRING," +
                "url STRING," +
                "pv INT" +
                ") WITH (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://10.130.34.101:3306/test'," +
                "'table-name' = 't_click_sink'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'username' = 'andy'," +
                "'password' = '123456')";

        // 4.1 将t_output注册到环境中
        tableEnvironment.executeSql(outTable);

        // 5. 使用SQL进行转换,获取t_input表所有数据,对于已经注册到环境中的表，可以以字符串的方式使用,否则需要以对象的方式使用。
        Table inputStream = tableEnvironment.sqlQuery("select * from t_input");
        // 5.1 以对象的方式查询t_input表
//        Table inputStream2 = tableEnvironment.sqlQuery("select * from " + tableEnvironment.from("t_input"));

        // 5.2 如何想对inputStream进行sqlQuery操作，需要将inputStream注册到环境中
        tableEnvironment.createTemporaryView("t_input2", inputStream);
        Table inputStream2 = tableEnvironment.sqlQuery("select * from t_input2 where user = 'Andy'");

        // 6. 将数据写入t_output表
        inputStream2.executeInsert("t_output");
//        inputStream2.executeInsert("t_output");

    }
}
