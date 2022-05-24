package org.leave.flink.practice.orderpay.bean;

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
}
