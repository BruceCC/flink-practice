package org.leave.flink.practice.goods.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/13 23:25
 * 用户行为
 */
@Data
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}
