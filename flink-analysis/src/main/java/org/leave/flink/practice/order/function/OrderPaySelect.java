package org.leave.flink.practice.order.function;

import org.apache.flink.cep.PatternSelectFunction;
import org.leave.flink.practice.order.bean.OrderEvent;
import org.leave.flink.practice.order.bean.OrderResult;

import java.util.List;
import java.util.Map;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 */
public class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {
    @Override
    public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
        // 匹配上了 begin follow都行
        Long payedOrderId = pattern.get("follow").iterator().next().getOrderId();
        return new OrderResult(payedOrderId, "paid successfully");
    }
}
