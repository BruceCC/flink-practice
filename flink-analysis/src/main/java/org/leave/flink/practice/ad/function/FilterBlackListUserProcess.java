package org.leave.flink.practice.ad.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.leave.flink.practice.ad.bean.AdClickEvent;
import org.leave.flink.practice.ad.bean.BlackListWarning;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/24
 */
public class FilterBlackListUserProcess extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent> {
    /**
     * 点击阈值
     */
    private Long maxClickNumber;

    /**
     * 旁流输出黑名单告警提醒
     */
    private OutputTag<BlackListWarning> blackListOutputTag;

    /**
     * 当前用户对当前广告的点击量
     */
    private ValueState<Long> countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState", Long.class));

    /**
     * 是否发送过黑名单状态
     */
    private ValueState<Boolean> isSentBlacklist = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSentBlacklist", Boolean.class));

    /**
     * 定时器出发的时间戳
     */
    private ValueState<Long> resetTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("resetTimer", Long.class));

    public FilterBlackListUserProcess(Long maxClickNumber, OutputTag<BlackListWarning> blackListOutputTag) {
        this.maxClickNumber = maxClickNumber;
        this.blackListOutputTag = blackListOutputTag;
    }

    @Override
    public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
        Long currentCount = countState.value();
        //如果是第一次处理，注册定时器每天00:00触发
        if (0 == currentCount) {
            Long ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24);
            resetTimer.update(ts);
            ctx.timerService().registerProcessingTimeTimer(ts);
        }

        //判断是否达到上限，达到则加入黑名单
        if (currentCount > maxClickNumber) {
            // 判断是否发送过黑名单，只发送一次
            if (!isSentBlacklist.value()) {
                isSentBlacklist.update(true);
                ctx.output(blackListOutputTag, new BlackListWarning(value.getUserId(), value.getAdId(), "click over " + maxClickNumber + " today"));
            }
        } else {
            countState.update(currentCount + 1);
            out.collect(value);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
        //定时器出发时清空状态
        if (timestamp == resetTimer.value()) {
            isSentBlacklist.clear();
            resetTimer.clear();
            countState.clear();
        }

    }
}
