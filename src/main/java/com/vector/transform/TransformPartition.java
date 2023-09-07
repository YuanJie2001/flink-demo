package com.vector.transform;

import com.vector.bean.SensorReadingEntity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.transform
 * @className com.vector.transform.TransformPartition
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/23 15:59
 */
public class TransformPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        FileSource<String> build = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path(System.getProperty("user.dir") + "/src/main/resources/source.txt"))
                .build();
        DataStream<String> streamSource = env.fromSource(build,
                WatermarkStrategy.noWatermarks(),
                "source.txt");

        streamSource.print("input");

        // 1. shuffle
        DataStream<String> shuffle = streamSource.shuffle();
        shuffle.print("shuffle");
        // 2. rebalance
        DataStream<String> rebalance = streamSource.rebalance();
        rebalance.print("rebalance");
        // 3. rescale
        DataStream<String> rescale = streamSource.rescale();
        rescale.print("rescale");
        // 4. global
        DataStream<String> global = streamSource.global();
        global.print("global");
        // 5. broadcast
        DataStream<String> broadcast = streamSource.broadcast();
        broadcast.print("broadcast");
        // 6. forward
        DataStream<String> forward = streamSource.forward();
        forward.print("forward");
        // 7. keyBy
        streamSource.keyBy(item -> {
            String[] split = item.split(",");
            return split[0];
        }).print("keyBy");
        env.execute();
    }
}
