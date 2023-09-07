package com.vector.apitest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.apitest
 * @className com.vector.apitest.SourceTest3_kafka
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/17 14:59
 */
public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String brokers = "172.27.188.96:9092";
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("aaaaa")
//                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.latest()) // 从最早的数据开始读取
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 只需要value
                .build();

        DataStream<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 打印
        kafkaSource.print();
        // 执行
        env.execute();

    }
}
