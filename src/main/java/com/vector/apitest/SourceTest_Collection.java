package com.vector.apitest;

import com.vector.bean.SensorReadingEntity;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.apitest
 * @className com.vector.apitest.SourceTest_Collection
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/17 11:27
 */
public class SourceTest_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReadingEntity> dataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReadingEntity("sensor_1", 1547718199L, 35.8),
                        new SensorReadingEntity("sensor_2", 1547718201L, 15.4),
                        new SensorReadingEntity("sensor_3", 1547718202L, 6.7),
                        new SensorReadingEntity("sensor_4", 1547718205L, 38.1)
                )
        );

        DataStream<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);

        // 打印输出
        dataStream.print("data");
        integerDataStreamSource.print("int");

        // 执行
        env.execute("job01");

    }
}
