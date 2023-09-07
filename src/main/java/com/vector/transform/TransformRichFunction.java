package com.vector.transform;

import com.vector.bean.SensorReadingEntity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.transform
 * @className com.vector.transform.TransformRichFunction
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/23 14:26
 */
public class TransformRichFunction {
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
        // 转换成SensorReading 类型
        DataStream<SensorReadingEntity> dataStream = streamSource.map(item -> {
            String[] split = item.split(",");
            return new SensorReadingEntity(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapFunction());
        resultStream.print("MyMapFunction");

        DataStream<Tuple2<String, Integer>> resultRichStream = dataStream.map(new MyRichMapFunction());
        resultRichStream.print("MyRichMapFunction");
        env.execute();
    }

    public static class MyMapFunction implements MapFunction<SensorReadingEntity, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReadingEntity value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    public static class MyRichMapFunction extends RichMapFunction<SensorReadingEntity,Tuple2<String,Integer>>{

            @Override
            public Tuple2<String, Integer> map(SensorReadingEntity value) throws Exception {
                return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                // 初始化工作，一般是定义状态，或者建立数据库连接
                // 每个并行实例都会调用一次
                System.out.println("open");
            }

            @Override
            public void close() throws Exception {
                // 一般是关闭连接和清空状态的收尾操作
                // 每个并行实例都会调用一次
                System.out.println("close");;
            }
    }

}
