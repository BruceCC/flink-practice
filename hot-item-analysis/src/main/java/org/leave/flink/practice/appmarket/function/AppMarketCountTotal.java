package org.leave.flink.practice.appmarket.function;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.appmarket.bean.AppMarketViewCount;

import java.sql.Timestamp;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/19
 */
public class AppMarketCountTotal implements WindowFunction<Long, AppMarketViewCount, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AppMarketViewCount> out) throws Exception {
        out.collect(new AppMarketViewCount(new Timestamp(window.getStart()).toString(), new Timestamp(window.getStart()).toString(), "App market", "total", input.iterator().next()));
    }
}
