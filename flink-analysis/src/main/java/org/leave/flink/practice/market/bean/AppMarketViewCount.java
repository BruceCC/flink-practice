package org.leave.flink.practice.market.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:12
 * 市场浏览统计
 */
@Data
public class AppMarketViewCount {
    private String windowStart;
    private String windowEnd;
    private String channel;
    private String behavior;
    private Long count;

    public AppMarketViewCount(String windowStart, String windowEnd, String channel, String behavior, Long count) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.channel = channel;
        this.behavior = behavior;
        this.count = count;
    }
}
