package org.leave.flink.practice.order.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:19
 * 订单支付账单事件
 */
@Data
public class ReceiptEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;

    public ReceiptEvent(String txId, String payChannel, Long eventTime) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.eventTime = eventTime;
    }
}
