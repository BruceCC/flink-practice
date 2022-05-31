package org.leave.flink.practice.page.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/31
 */
public class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<Integer, Long> value, Long accumulator) {
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
