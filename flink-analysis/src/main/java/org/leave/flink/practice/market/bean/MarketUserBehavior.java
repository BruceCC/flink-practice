package org.leave.flink.practice.market.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:10
 * 市场用户行为
 */
@Data
public class MarketUserBehavior {
    private String userId;
    private String behavior;
    private String channel;
    private Long timestamp;

    public MarketUserBehavior(String userId, String behavior, String channel, Long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }
}
