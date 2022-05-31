package org.leave.flink.practice.page.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.page.bean.PageViewCount;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/31
 * 实现自定义处理函数，把相同窗口分组统计的count值叠加
 */
public class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {
    /**
     * 定义状态，保存当前的总count值
     */
    private ValueState<Long> totalCountState;

    @Override
    public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
        Long totalCount = totalCountState.value();
        if(null == totalCount){
            totalCount = 0L;
            totalCountState.update(totalCount);
        }
        totalCountState.update( totalCount + value.getCount() );
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
        // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
        Long totalCount = totalCountState.value();
        out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
        // 清空状态
        totalCountState.clear();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count", Long.class));
    }
}
