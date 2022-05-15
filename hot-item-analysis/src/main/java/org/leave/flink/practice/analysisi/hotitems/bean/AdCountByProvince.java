package org.leave.flink.practice.analysisi.hotitems.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:06
 * 按照省份统计输出结果
 */
@Data
public class AdCountByProvince {
    private String windowStart;
    private String windowEnd;
    private String province;
    private Long count;
}
