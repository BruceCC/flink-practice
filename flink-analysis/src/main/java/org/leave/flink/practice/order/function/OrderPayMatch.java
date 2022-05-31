package org.leave.flink.practice.order.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.leave.flink.practice.order.bean.OrderEvent;
import org.leave.flink.practice.order.bean.OrderResult;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 * 所有的匹配处理
 */
public class OrderPayMatch extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {
    private OutputTag<OrderResult> orderTimeoutOutputTag;
    /**
     *保存pay是否来过的状态
     */
    private ValueState<Boolean> isPaidState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isPaidState", Boolean.class));
    /**
     * 保存定时器的时间戳
     */
    private ValueState<Long> timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));

    public OrderPayMatch(OutputTag<OrderResult> orderTimeoutOutputTag) {
        this.orderTimeoutOutputTag = orderTimeoutOutputTag;
    }

    @Override
    public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
        Boolean isPaid = isPaidState.value();
        Long timerTs = timerState.value();
        if ("create".equalsIgnoreCase(value.getEventType())) {
            // 如果是create，接下来判断是否有pay来过
            if (isPaid) {
                // 如果已经来过，匹配成功，输出主流，清空状态
                out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                ctx.timerService().deleteEventTimeTimer(timerTs);
                isPaidState.clear();
                timerState.clear();
            } else {
                // 如果没有pay 注册定时器 等待pay到来
                Long ts = timerTs + 15 * 60 * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerState.update(ts);
            }
        } else if ("pay".equalsIgnoreCase(value.getEventType())) {
            // 如果是pay事件，先判断是否create过，用timer判断
            if (timerTs > 0) {
                // 有定时器，说明已经有create过, 继续判断，是否超过了timeout时间
                if (timerTs > value.getEventTime()) {
                    // 表示定时器时间还没到，pay就来了，成功匹配
                    out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                } else {
                    // 超时
                    ctx.output(orderTimeoutOutputTag, new OrderResult(ctx.getCurrentKey(), "payed but already timeout"));
                }
                // 输出状态 清空状态
                ctx.timerService().deleteEventTimeTimer(timerTs);
                isPaidState.clear();
                timerState.clear();
            }
        } else {
            // 没有create 直接先来了pay，先更新状态，注册定时器等待create,  相当于乱序了，这里直接注册当前的eventTime，等watermask触发
            isPaidState.update(true);
            ctx.timerService().registerEventTimeTimer(value.getEventTime());
            timerState.update(value.getEventTime());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
        // 根据状态的值，判断哪个数据没来
        if (isPaidState.value()) {
            //如果true 表示pay先到了，没等到create
            ctx.output(orderTimeoutOutputTag, new OrderResult(ctx.getCurrentKey(), "already payed buy not found create"));
        } else {
            // 有create了，没有pay
            ctx.output(orderTimeoutOutputTag, new OrderResult(ctx.getCurrentKey(), "time out"));
        }
        isPaidState.clear();
        timerState.clear();
    }
}
