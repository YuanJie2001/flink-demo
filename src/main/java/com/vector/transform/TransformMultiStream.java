package com.vector.transform;

import com.vector.bean.SensorReadingEntity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.transform
 * @className com.vector.transform.TransformMultiStream
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/22 10:52
 */
public class TransformMultiStream {
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

        // 分流 侧输出流 定义低温流标识
        final OutputTag<SensorReadingEntity> outputTag = new OutputTag<SensorReadingEntity>("low") {};
        SingleOutputStreamOperator<SensorReadingEntity> process = dataStream.process(new ProcessFunction<SensorReadingEntity, SensorReadingEntity>() {
            @Override
            public void processElement(SensorReadingEntity sensorReadingEntity, ProcessFunction<SensorReadingEntity, SensorReadingEntity>.Context context, Collector<SensorReadingEntity> collector) throws Exception {
                if (sensorReadingEntity.getTemperature() > 30) {
                    collector.collect(sensorReadingEntity);
                } else {
                    context.output(outputTag, sensorReadingEntity);
                }
            }
        });

        // 低温流 侧输出流
        process.getSideOutput(outputTag).print("low");
        // 高温流 主流
        process.print("high");


        // 连接流(数据类型不同) + 合流(完全相同) 转换为二元组
        DataStream<Object> map = process.map(item -> {
                    SensorReadingEntity sensorReadingEntity = (SensorReadingEntity) item;
                    return new Tuple2<>(sensorReadingEntity.getId(), sensorReadingEntity.getTemperature());
                })
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .connect(process.getSideOutput(outputTag))
                .map(new CoMapFunction<Tuple2<String, Double>, SensorReadingEntity, Object>() {
                    @Override
                    public Object map1(Tuple2<String, Double> tuple2) throws Exception {
                        return new Tuple3<>(tuple2.f0, tuple2.f1, "高温报警");
                    }
                    @Override
                    public Object map2(SensorReadingEntity sensorReadingEntity) throws Exception {
                        return new Tuple2<>(sensorReadingEntity.getId(), "正常");
                    }
                });
        map.print("connect");


        // union 合流
        DataStream<SensorReadingEntity> union = process.union(process.getSideOutput(outputTag));
        union.print("union");
        env.execute();
    }
}
