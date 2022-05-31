package org.leave.flink.practice.page.function;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.page.bean.PageViewCount;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/31
 */
public class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
    @Override
    public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
        out.collect( new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()) );
    }
}
