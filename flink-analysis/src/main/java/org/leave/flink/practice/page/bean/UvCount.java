package org.leave.flink.practice.page.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:13
 */
@Data
public class UvCount {
    private Long windowEnd;
    private Long uvCount;

    public UvCount(Long windowEnd, Long uvCount) {
        this.windowEnd = windowEnd;
        this.uvCount = uvCount;
    }
}
