package org.leave.flink.practice.analysisi.hotitems.function;

import org.apache.flink.cep.PatternSelectFunction;
import org.leave.flink.practice.analysisi.hotitems.bean.LoginEvent;
import org.leave.flink.practice.analysisi.hotitems.bean.LoginWarning;

import java.util.List;
import java.util.Map;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 17:56
 */
public class LoginFailMatch implements PatternSelectFunction<LoginEvent, LoginWarning> {
    @Override
    public LoginWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
        // 按照name取出对应事件
        LoginEvent firstFail = pattern.get("begin").iterator().next();
        LoginEvent lastFail = pattern.get("next").iterator().next();
        LoginWarning loginWarning = new LoginWarning();
        loginWarning.setUserId(firstFail.getUserId());
        loginWarning.setFirstFailTime(firstFail.getEventTime());
        loginWarning.setLastFailTime(lastFail.getEventTime());
        loginWarning.setWarningMsg("login fail");
        return loginWarning;
    }
}
