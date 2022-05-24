package org.leave.flink.practice.login.bean;

import lombok.Data;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:03
 * 登录警告
 */
@Data
public class LoginWarning {
    private Long userId;
    private Long firstFailTime;
    private Long lastFailTime;
    private String warningMsg;
}
