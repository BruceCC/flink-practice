package org.leave.flink.practice.analysisi.hotitems.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:04
 * 广告点击事件
 */
@Data
public class AdClickEvent {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private String timestamp;
}
