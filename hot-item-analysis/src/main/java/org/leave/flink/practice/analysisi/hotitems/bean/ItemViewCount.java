package org.leave.flink.practice.analysisi.hotitems.bean;

import lombok.Data;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/6
 */
@Data
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
