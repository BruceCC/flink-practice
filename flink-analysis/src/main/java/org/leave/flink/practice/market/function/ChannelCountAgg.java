package org.leave.flink.practice.market.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.leave.flink.practice.market.bean.MarketUserBehavior;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/19
 */
public class ChannelCountAgg implements AggregateFunction<MarketUserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(MarketUserBehavior value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
