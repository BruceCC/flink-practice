package org.leave.flink.practice.ad.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.leave.flink.practice.ad.bean.AdClickEvent;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/24
 */
public class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(AdClickEvent value, Long accumulator) {
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
