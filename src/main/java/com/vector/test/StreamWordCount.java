package com.vector.test;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector
 * @className com.vector.SreamWordCount
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/5/9 20:45
 */

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(4);
        // 从文件中读取数据 有界流
//        String inputPath = System.getProperty("user.dir") + "/src/main/resources/text.txt";
//        FileSource<String> source.txt = FileSource
//                .forRecordStreamFormat(
//                        new TextLineInputFormat("UTF-8"),
//                        new Path(inputPath))
//                        .build();
//        DataStream<String> inputDataStream =
//                env.fromSource(source.txt, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "text");

//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");
        // 从socket文本流读取数据 nc -lk 7777 无界流
        DataStreamSource<String> inputDataStream =
                env.socketTextStream("localhost", 7777);
        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultSet =
                inputDataStream.flatMap(new WordCount.MyFlatMapper()).slotSharingGroup("02")
                        .keyBy(KeySelector -> KeySelector.f0)
                        .sum(1)
                        .setParallelism(4)
                        .slotSharingGroup("01");
        resultSet.print();
        // 执行任务
        env.execute();
    }


}
