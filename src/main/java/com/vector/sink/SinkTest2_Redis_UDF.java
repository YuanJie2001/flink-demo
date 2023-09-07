package com.vector.sink;

import com.vector.bean.SensorReadingEntity;
import com.vector.config.RedissonConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import java.util.concurrent.TimeUnit;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.sink
 * @className com.vector.sink.SinkTest2_Redis
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/24 9:17
 */
public class SinkTest2_Redis_UDF {
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

        dataStream.addSink(new RedisSink_UDF());

        env.execute();
    }


    public static class RedisSink_UDF extends RichSinkFunction<SensorReadingEntity> {
        RedissonClient redisClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            // springboot不需要这样获取,直接注入redisson配置类
            super.open(parameters);
            redisClient = RedissonConfig.redisson();
        }

        @Override
        public void close() throws Exception {
            super.close();
            redisClient.shutdown();
        }

        @Override
        public void invoke(SensorReadingEntity sensorReadingEntity, Context context) throws Exception {
            if(redisClient == null){
                redisClient = RedissonConfig.redisson();
            }
            System.out.println(sensorReadingEntity);
            RMap<String, SensorReadingEntity> map = redisClient.getMap("real-time-key");
            map.expire(10, TimeUnit.MINUTES);
            map.put(sensorReadingEntity.getId(),sensorReadingEntity);
        }
    }
}
