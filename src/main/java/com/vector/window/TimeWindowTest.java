package com.vector.window;

import com.vector.bean.SensorReadingEntity;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.window
 * @className com.vector.window.TimeWindow
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/24 16:32
 */
public class TimeWindowTest {
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

        // 窗口测试
        // dataStream.windowAll(); //global
//        DataStream<Integer> resultStream = dataStream
//                .keyBy(SensorReadingEntity::getId)
//                // 滚动窗口 参数1为一个窗口1分钟s接收时间,参数2时间偏移15s开一个窗口
//                .window(TumblingProcessingTimeWindows.of((Time.minutes(1)), Time.seconds(15)))
//                .aggregate(new AggregateFunction<SensorReadingEntity, AtomicInteger, Integer>() {
//                    @Override
//                    public AtomicInteger createAccumulator() {
//                        return new AtomicInteger(0);
//                    }
//
//                    @Override
//                    public AtomicInteger add(SensorReadingEntity sensorReadingEntity, AtomicInteger atomicInteger) {
//                        atomicInteger.incrementAndGet();
//                        return atomicInteger;
//                    }
//
//                    @Override
//                    public Integer getResult(AtomicInteger atomicInteger) {
//                        return atomicInteger.get();
//                    }
//
//                    @Override
//                    public AtomicInteger merge(AtomicInteger atomicInteger, AtomicInteger acc1) {
//                        int i = atomicInteger.get() + acc1.get();
//                        atomicInteger.set(i);
//                        return atomicInteger;
//                    }
//                });
//        resultStream.print();
        // 全窗口函数计算
//        DataStream<Tuple3<String,Long,Integer>> apply = dataStream.keyBy(SensorReadingEntity::getId)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//                // apply方法中的参数1为输入,参数2为输出,参数3为上一步聚合key,参数4为窗口
//                .apply(new WindowFunction<SensorReadingEntity, Tuple3<String,Long,Integer>, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow window, Iterable<SensorReadingEntity> input, Collector<Tuple3<String,Long,Integer>> out) throws Exception {
//
//                        int count = 0;
//                        for (SensorReadingEntity ignored : input) {
//                            count++;
//                        }
//                        out.collect(new Tuple3<>(s,window.getEnd(),count));
//                    }
//                });
//        apply.print();


//        dataStream.keyBy(SensorReadingEntity::getId)
//                // 滑动窗口 1个窗口30s接收时间,窗口间隔15s 开一个窗口
//                        .window(SlidingProcessingTimeWindows.of(Time.seconds(30),Time.seconds(15)));
//        dataStream.keyBy(SensorReadingEntity::getId)
//                // session窗口
//                        .window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)));
//
//        dataStream.keyBy(SensorReadingEntity::getId)
//                // 滚动统计窗口
//                        .countWindow(5);
//        dataStream.keyBy(SensorReadingEntity::getId)
//                // 滑动统计窗口
//                        .countWindow(5,3);
        env.execute();
    }
}
