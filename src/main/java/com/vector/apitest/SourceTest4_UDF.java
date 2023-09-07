package com.vector.apitest;

import com.vector.bean.SensorReadingEntity;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.apitest
 * @className com.vector.apitest.SourceTest4_UDF
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/21 15:15
 */
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从自定义的数据源读取数据
        DataStream<SensorReadingEntity> dataStream = env.addSource(new MySensorSource());
        // 打印输出
        dataStream.print();
        // 执行
        env.execute();


    }

    // 实现自定义的SourceFunction<OUT>接口，自定义的数据源
    private static class MySensorSource implements SourceFunction<SensorReadingEntity> {
        // 定义一个标志位，用来表示数据源是否正常运行发出数据
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReadingEntity> sourceContext) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();
            // 设置10个传感器的初始温度 0~120℃正态分布
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }
            while (running) {
                // 在while循环中，随机生成SensorReading数据
                for (String sensorId : sensorTempMap.keySet()) {
                    // 在当前温度基础上随机波动
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    sourceContext.collect(new SensorReadingEntity(sensorId, System.currentTimeMillis(), newTemp));
                }
                // 每隔1秒钟发送一次传感器数据
                TimeUnit.MILLISECONDS.sleep(1000L);
            }

        }
        @Override
        public void cancel() {
            running = false;
        }
    }
}
