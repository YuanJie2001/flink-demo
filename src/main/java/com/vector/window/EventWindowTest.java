package com.vector.window;

import com.vector.bean.SensorReadingEntity;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.window
 * @className com.vector.window.EventWindowTest
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/25 16:38
 */
public class EventWindowTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // socketText  nc -lk 7777
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading 类型
        DataStream<SensorReadingEntity> dataStream = streamSource.map(item -> {
            String[] split = item.split(",");
            return new SensorReadingEntity(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });
    }
}
