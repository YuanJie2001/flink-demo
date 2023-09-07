package com.vector.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

/**
 * @author WangJiaHui
 * @description: test
 * @ClassName RedissonConfig
 * @date 2022/3/5 10:11
 */
@Slf4j
public class RedissonConfig {

    public static RedissonClient redisson() {
        Config config = new Config();
        //config.useClusterServers().addNodeAddress("127.0.0.1:6379").setPassword("123456");
        config.useSingleServer()
                .setAddress("redis://"+ "localhost:6379") // 单机模式
                .setPassword("123456") // 密码
                .setSubscriptionConnectionMinimumIdleSize(1) // 对于支持多个Redis连接的RedissonClient对象，
                .setSubscriptionConnectionPoolSize(50) // 对于支持绑定多个Redisson连接的RedissonClient对象，
                .setConnectionMinimumIdleSize(32) // 最小空闲连接数
                .setConnectionPoolSize(64) // 只能用于单机模式
                .setDnsMonitoringInterval(5000) // DNS监控间隔时间，单位：毫秒
                .setIdleConnectionTimeout(10000) // 空闲连接超时时间，单位：毫秒
                .setConnectTimeout(10000) // 连接超时时间，单位：毫秒
                .setPingConnectionInterval(10000) // 集群状态扫描间隔时间，单位：毫秒
                .setTimeout(5000) // 命令等待超时时间，单位：毫秒
                .setRetryAttempts(3) // 命令重试次数
                .setRetryInterval(1500) // 命令重试发送时间间隔，单位：毫秒
                .setDatabase(0) // 数据库编号
                .setSubscriptionsPerConnection(5); // 每个连接的最大订阅数量
        config.setCodec(new JsonJacksonCodec()); // 设置编码方式
        return Redisson.create(config);
    }
}