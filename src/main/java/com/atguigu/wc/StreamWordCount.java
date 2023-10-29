package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: StreamWordCount
 * Package: com.atguigu.wc
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/10/22 18:23
 * @Version 1.0
 */
public class StreamWordCount {
    public static void main(String[] args) {
        // 1. 创建流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // new 从命令行参数获取hostname和port
        // idea在Run->Edit Configurations->Program arguments中输入 --hostname localhost --port 7777
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        // 2. 读取文本流
        // 需要使用 $ nc -lk 7777 先监听这个端口
        DataStreamSource<String> linegDataStreamSource = env.socketTextStream(hostname,port);

        // 3.转换计算
        linegDataStreamSource.flatMap((String line, Collector<Tuple2<String,Long>> out) -> {
                    // 将一行文本进行分词
                    String[] words = line.split(" ");
                    // 将每个单词转成二元组输出
                    for (String word: words) {
                        out.collect(Tuple2.of(word,1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                // 按照word进行分组
                .keyBy(0)
                // 按照第二个元素进行聚合
                .sum(1)
                // 打印结果
                .print();
        // 4. 执行任务(相比DataSet接口需要多这个步骤,以保证一直读取文件中的数据)
        try {
            // 启动任务后，在nc的控制台输入数据，就可以看到结果了
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
