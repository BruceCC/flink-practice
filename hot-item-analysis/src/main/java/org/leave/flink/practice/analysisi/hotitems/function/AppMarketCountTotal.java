package org.leave.flink.practice.analysisi.hotitems.function;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.analysisi.hotitems.bean.AppMarketViewCount;

import java.sql.Timestamp;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/19
 */
public class AppMarketCountTotal implements WindowFunction<Long, AppMarketViewCount, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AppMarketViewCount> out) throws Exception {
        AppMarketViewCount appMarketViewCount = new AppMarketViewCount();
        appMarketViewCount.setWindowStart(new Timestamp(window.getStart()).toString());
        appMarketViewCount.setWindowEnd(new Timestamp(window.getEnd()).toString());
        appMarketViewCount.setChannel("app marketing");
        appMarketViewCount.setBehavior("total");
        appMarketViewCount.setCount(input.iterator().next());
        out.collect(appMarketViewCount);
    }
}
