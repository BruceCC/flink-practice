package org.leave.flink.practice.ad.bean;

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

    public AdCountByProvince(String windowStart, String windowEnd, String province, Long count) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.province = province;
        this.count = count;
    }
}
