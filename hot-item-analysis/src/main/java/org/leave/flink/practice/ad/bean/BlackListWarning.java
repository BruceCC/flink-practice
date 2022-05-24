package org.leave.flink.practice.ad.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:07
 * 黑名单报警信息
 */
@Data
public class BlackListWarning {
    private Long userId;
    private Long adId;
    private String msg;

    public BlackListWarning(Long userId, Long adId, String msg) {
        this.userId = userId;
        this.adId = adId;
        this.msg = msg;
    }
}
