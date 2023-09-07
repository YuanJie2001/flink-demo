package com.vector.window;

import com.vector.bean.SensorReadingEntity;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.window
 * @className com.vector.window.CountWindowTest
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/25 10:09
 */
public class CountWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // socketText  nc -lk 7777
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading 类型
        DataStream<SensorReadingEntity> dataStream = streamSource.map(item -> {
            String[] split = item.split(",");
            return new SensorReadingEntity(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        // 开窗计数窗口
        DataStream<Double> aggregate = dataStream.keyBy(SensorReadingEntity::getId)
                .countWindow(10, 2)
                .aggregate(new AvgTemp());
        aggregate.print();
        env.execute();
    }

    public static class AvgTemp implements AggregateFunction<SensorReadingEntity, Tuple2<Double,Integer>,Double> {
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReadingEntity sensorReadingEntity, Tuple2<Double, Integer> objects) {
            objects.f0 += sensorReadingEntity.getTemperature();
            objects.f1 += 1;
            return objects;
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> objects) {
            return objects.f0/objects.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> objects, Tuple2<Double, Integer> acc1) {
            return new Tuple2<>(objects.f0+acc1.f0,objects.f1+acc1.f1);
        }
    }
}
