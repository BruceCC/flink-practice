package org.leave.flink.practice.orderpay.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:18
 * 订单结果
 */
@Data
public class OrderResult {
    private Long orderId;
    private String resultMsg;
}
