package com.vector.apitest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.apitest
 * @className com.vector.apitest.SourceTest2_File
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/17 14:20
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 从文件中读取数据
        String inputPath = System.getProperty("user.dir") + "/src/main/resources/source.txt";


        FileSource<String> build = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
                .build();
        DataStream<String> dataStream = env
                .fromSource(build, WatermarkStrategy.noWatermarks(),"source.txt");
        // 打印输出
        dataStream.print();
        env.execute();


    }
}
