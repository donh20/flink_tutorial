package com.ncamc.wc;

import lombok.Value;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
* 任务定义好，然后用流的形式把数据灌进来
* */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //先创建一个流处理的执行环境，注意别引用成scala的
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(4); //默认并行度等同于当前CPU核心数量
        /*
        //从文件中读取数据
        String inputPath = "E:\\workspace\\flink\\src\\main\\resources\\hello.txt";
        //最核心的api还是DataStream
        DataStream<String> inputDataStream = env.readTextFile(inputPath);
        */

        //用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        //从socket文本流读取数据
        DataStreamSource<String> inputDataStream = env.socketTextStream(host, port);


        //基于数据流进行转换计算的任务
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();

        //执行任务
        env.execute();
    }
}
