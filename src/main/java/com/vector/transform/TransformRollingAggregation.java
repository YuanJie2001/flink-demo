package com.vector.transform;

import com.vector.bean.SensorReadingEntity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.transform
 * @className com.vector.transform.TransformRollingAggregation
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/22 9:24
 */
public class TransformRollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 从文件读取数据
        FileSource<String> build = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path(System.getProperty("user.dir") + "/src/main/resources/source.txt"))
                .build();
        DataStream<String> streamSource = env.fromSource(build,
                WatermarkStrategy.noWatermarks(),
                "source.txt");
        // 转换成SensorReading 类型
        DataStream<SensorReadingEntity> dataStream = streamSource.map(item -> {
            String[] split = item.split(",");
            return new SensorReadingEntity(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        // 分组 滚动聚合
        DataStream<SensorReadingEntity> temperature = dataStream.keyBy(SensorReadingEntity::getId)
                // 滚动聚合
                .maxBy("temperature");

        temperature.print("temperature");
        env.execute();
    }
}
