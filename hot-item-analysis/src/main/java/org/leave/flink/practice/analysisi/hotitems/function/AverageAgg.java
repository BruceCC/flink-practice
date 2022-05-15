package org.leave.flink.practice.analysisi.hotitems.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.leave.flink.practice.analysisi.hotitems.bean.UserBehavior;

/**
 * @author BruceCC Zhong
 * @date 2022/5/14 16:47
 * 自定义预聚合函数计算平均数的实例
 */
public class AverageAgg implements AggregateFunction<UserBehavior, Tuple2<Long, Integer>, Double> {
    @Override
    public Tuple2<Long, Integer> createAccumulator() {
        return Tuple2.of(0L, 0);
    }

    @Override
    public Tuple2<Long, Integer> add(UserBehavior value, Tuple2<Long, Integer> accumulator) {
        return Tuple2.of(accumulator.f0 + value.getTimestamp(), accumulator.f1 + 1);
    }

    @Override
    public Double getResult(Tuple2<Long, Integer> accumulator) {
        return Double.valueOf(accumulator.f0 / accumulator.f1);
    }

    @Override
    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}
