package org.leave.flink.practice.ad.bean;

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
    private Long timestamp;

    public AdClickEvent(Long userId, Long adId, String province, String city, Long timestamp) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }
}
