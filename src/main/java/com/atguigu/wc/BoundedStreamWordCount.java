package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;

/**
 * ClassName: BoundedStreamWordCount
 * Package: com.atguigu.wc
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/10/22 18:04
 * @Version 1.0
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) {
        // 1. 创建流式的执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件中读取数据
        String inputPath = "input/words.txt";
        DataStreamSource<String> linegDataStreamSource = executionEnvironment.readTextFile(inputPath);

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
            executionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
