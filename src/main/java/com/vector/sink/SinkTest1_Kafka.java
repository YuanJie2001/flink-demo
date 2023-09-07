package com.vector.sink;

import com.vector.bean.SensorReadingEntity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.sink
 * @className com.vector.sink.SinkTest1_Kafka
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/23 16:30
 */
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        String brokers = "172.27.181.61:9092";
        // 从kafka读取数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("topic-producer")
                .setStartingOffsets(OffsetsInitializer.latest()) // 从最新的数据开始读取
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 只需要value
                .build();

        DataStream<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 转换成SensorReading 类型
        DataStream<String> dataStream = kafkaSource.map(item -> {
            String[] split = item.split(",");
            try {
                return new SensorReadingEntity(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2])).toString();
            } catch (Exception e) {
                return item;
            }
        });


        // 输出到kafka
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic-sink")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // 至少一次
                .build();
        dataStream.sinkTo(sink);

        env.execute();
    }
}
