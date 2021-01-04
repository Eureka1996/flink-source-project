package com.wufuqiang.fsource.mysql;

import com.wufuqiang.entities.Website;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Main {
    public static void main(String[] args) throws Exception {
        SourceFunction<Website> source = MySqlJDBCSource.<Website>builder()
                .hostname("localhost")
                .username("root")
                .password("iamroot")
                .dbname("test1")
                .sql("select id,name,url,alexa,country from websites where id > 10;")
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(source).print().setParallelism(2);

        env.execute();
    }

}
