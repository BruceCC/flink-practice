package org.leave.flink.practice.order.function;

import org.apache.flink.cep.PatternTimeoutFunction;
import org.leave.flink.practice.order.bean.OrderEvent;
import org.leave.flink.practice.order.bean.OrderResult;

import java.util.List;
import java.util.Map;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 * 自定义超时事件序列 处理函数 即订单超时
 */
public class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {
    @Override
    public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
        Long timeoutOrderId = pattern.get("begin").iterator().next().getOrderId();
        return new OrderResult(timeoutOrderId, "timeout");
    }
}
