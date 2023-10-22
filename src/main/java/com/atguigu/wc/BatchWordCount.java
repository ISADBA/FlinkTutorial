package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * ClassName: BatchWordCount
 * Package: com.atguigu.wc
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/10/22 17:17
 * @Version 1.0
 */

// 这个batch处理实现是通过DataSet实现的
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文件中读取数据
        String inputPath = "input/words.txt";
        DataSource<String> lineDataSource = env.readTextFile(inputPath);
        // 3. 对数据进行分词，再按照word进行分组
        FlatMapOperator<String,Tuple2<String,Long>> wordAndOneTuple =  lineDataSource.flatMap((String line, Collector<Tuple2<String,Long>> out) -> {
            // 将一行文本进行粉刺
            String[] words = line.split(" ");
            // 将每个单词转成二元组输出
            for (String word: words) {
                out.collect(Tuple2.of(word,1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING,Types.LONG));
        // 4. 按照word进行分组
        UnsortedGrouping<Tuple2<String,Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

        // 5. 对分组后的数据进行聚合
        AggregateOperator<Tuple2<String,Long>> result = wordAndOneGroup.sum(1);

        // 6. 打印结果
        result.print();
    }

}
