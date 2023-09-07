package com.vector.transform;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.transform
 * @className com.vector.transform.TransformTest1
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/21 16:59
 */
public class TransformTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 从文件读数据
        FileSource<String> build = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path(System.getProperty("user.dir") + "/src/main/resources/source.txt"))
                .build();

        // map 转换成长度输出
        DataStream<Integer> map = env.fromSource(build,  WatermarkStrategy.noWatermarks(), "source.txt")
                .map(String::length);
        // flatMap 按逗号分隔
        DataStream<String> flatMap = env.fromSource(build,  WatermarkStrategy.noWatermarks(), "source.txt")
                .flatMap((String s,Collector<String> collector) -> {
                    String[] split = s.split(",");
                    for (String s1 : split) {
                        collector.collect(s1);
                    }
                })
                .returns(Types.STRING);

        // filter 筛选 sensor_1 开头的id
        DataStream<String> filter = env.fromSource(build,  WatermarkStrategy.noWatermarks(), "source.txt")
                .filter(s -> s.startsWith("sensor_1"));

        map.print("map");
        flatMap.print("flatMap");
        filter.print("filter");

        env.execute();

    }
}
