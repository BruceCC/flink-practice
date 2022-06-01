package org.leave.flink.practice.market.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.market.bean.MarketViewCount;

import java.sql.Timestamp;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/19
 */
public class MarketCountTotal implements WindowFunction<Long, MarketViewCount, Tuple2<String, String>, TimeWindow> {
    @Override
    public void apply(Tuple2<String, String> stringStringTuple2, TimeWindow window, Iterable<Long> input, Collector<MarketViewCount> out) throws Exception {
        out.collect(new MarketViewCount(new Timestamp(window.getStart()).toString(), new Timestamp(window.getStart()).toString(), stringStringTuple2.f0, stringStringTuple2.f1, input.iterator().next()));
    }
}
