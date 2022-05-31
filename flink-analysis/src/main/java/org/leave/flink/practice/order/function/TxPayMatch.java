package org.leave.flink.practice.order.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.leave.flink.practice.order.bean.OrderEvent;
import org.leave.flink.practice.order.bean.ReceiptEvent;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/26
 */
public class TxPayMatch extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
    private OutputTag<OrderEvent> unmatchedPay;
    private OutputTag<ReceiptEvent> unmatchedReceipt;

    /**
     * 定义状态来保存已经到达的订单支付事件和到账事件
     */
    private ValueState<OrderEvent> payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payState", OrderEvent.class));
    private ValueState<ReceiptEvent> receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receiptState", ReceiptEvent.class));

    public TxPayMatch(OutputTag<OrderEvent> unmatchedPay, OutputTag<ReceiptEvent> unmatchedReceipt) {
        this.unmatchedPay = unmatchedPay;
        this.unmatchedReceipt = unmatchedReceipt;
    }

    /**
     * 订单支付事件数据的处理
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        // 判断有没有对应的到账事件
        ReceiptEvent receiptEvent = receiptState.value();
        if (null != receiptEvent) {
            // 如果已经有receipt，在主流中输出 清空到账状态
            out.collect(Tuple2.of(value, receiptEvent));
            receiptState.clear();
        } else {
            // 如果还没到，那么把pay存入状态，并且注册一个定时器等待5s
            payState.update(value);
            ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000);
        }
    }

    /**
     * 到账事件的处理
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        OrderEvent orderEvent = payState.value();
        if (null != orderEvent) {
            out.collect(Tuple2.of(orderEvent, value));
            payState.clear();
        } else {
            receiptState.update(value);
            ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        // 到时间了，如果还没收到某个事件，就输出报警
        OrderEvent orderEvent = payState.value();
        if (null != orderEvent) {
            ctx.output(unmatchedPay, orderEvent);
        }

        ReceiptEvent receiptEvent = receiptState.value();
        if (null != receiptEvent) {
            ctx.output(unmatchedReceipt, receiptEvent);
        }

        payState.clear();
        receiptState.clear();
    }
}
