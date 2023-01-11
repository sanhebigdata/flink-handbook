package com.sanhe.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class DataStreamWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSource<String> textSource = env.fromElements("顺，不妄喜；逆，不惶馁；安，不奢逸；危，不惊惧；胸有惊雷而面如平湖者，可拜上将军。");
        FlatMapOperator<String, Tuple2<String, Integer>> resultOperator = textSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String str, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for( String word : str.split("")) {
                    collector.collect(new Tuple2<>(word,1));
                }
            }
        });

        resultOperator.groupBy(0).sum(1).print();

    }
}
