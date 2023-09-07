package com.vector.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author YuanJie
 * @projectName flink
 * @package com.vector.apitest.bean
 * @className com.vector.apitest.bean.SensorReading
 * @copyright Copyright 2020 vector, Inc All rights reserved.
 * @date 2023/8/17 11:29
 */

// 传感器温度读数数据类型
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReadingEntity {
    private String id;
    private Long timestamp;
    private Double temperature;
}
