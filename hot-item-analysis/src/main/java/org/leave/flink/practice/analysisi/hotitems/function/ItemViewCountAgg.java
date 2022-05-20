package org.leave.flink.practice.analysisi.hotitems.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.leave.flink.practice.analysisi.hotitems.bean.UserBehavior;

/**
 * @author BruceCC Zhong
 * @date 2022/5/14 16:42
 * 自定义预聚合函数
 */
public class ItemViewCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior value, Long accumulator) {
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
