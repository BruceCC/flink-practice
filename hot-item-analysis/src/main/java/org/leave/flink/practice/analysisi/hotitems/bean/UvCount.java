package org.leave.flink.practice.analysisi.hotitems.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:13
 */
@Data
public class UvCount {
    private Long windowEnd;
    private Long uvCount;
}
