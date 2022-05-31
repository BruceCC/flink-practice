package org.leave.flink.practice.order.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:14
 * 订单事件
 */
@Data
public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;

    public OrderEvent(Long orderId, String eventType, String txId, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.txId = txId;
        this.eventTime = eventTime;
    }
}
