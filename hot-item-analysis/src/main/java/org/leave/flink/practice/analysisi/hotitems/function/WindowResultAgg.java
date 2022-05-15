package org.leave.flink.practice.analysisi.hotitems.function;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.analysisi.hotitems.bean.ItemViewCount;

/**
 * @author BruceCC Zhong
 * @date 2022/5/14 17:18
 * 自定义窗口函数，输出ItemViewCount
 */
public class WindowResultAgg implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
    @Override
    public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) {
        ItemViewCount itemViewCount = new ItemViewCount();
        itemViewCount.setItemId(aLong);
        itemViewCount.setWindowEnd(window.getEnd());
        itemViewCount.setCount(input.iterator().next());
        out.collect(itemViewCount);
    }
}
