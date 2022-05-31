package org.leave.flink.practice.order.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.order.bean.OrderEvent;
import org.leave.flink.practice.order.bean.ReceiptEvent;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/26
 */
public class TxPayMatchByJoin extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
    @Override
    public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        out.collect(Tuple2.of(left, right));
    }
}
