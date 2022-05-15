package org.leave.flink.practice.analysisi.hotitems.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.analysisi.hotitems.bean.ItemViewCount;
import org.leave.flink.practice.analysisi.hotitems.bean.LoginEvent;
import org.leave.flink.practice.analysisi.hotitems.bean.LoginWarning;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:24
 */
public class LoginWarningProcess extends KeyedProcessFunction<Long, LoginEvent, LoginWarning> {
    private Integer maxFailNumber;
    /**
     * 定义状态，保存2秒内所有的登录失败事件
     */
    private ListState<LoginEvent> loginFailListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginFailListState", LoginEvent.class));

    public LoginWarningProcess(Integer maxFailNumber) {
        this.maxFailNumber = maxFailNumber;
    }


    @Override
    public void processElement(LoginEvent value, KeyedProcessFunction<Long, LoginEvent, LoginWarning>.Context ctx, Collector<LoginWarning> out) throws Exception {
        if ("fail".equalsIgnoreCase(value.getEventType())) {
            // 如果失败，判断之前是否有登录失败事件
            Iterator<LoginEvent> iterator = loginFailListState.get().iterator();
            if (iterator.hasNext()) {
                // 如果之前已经有登录失败事件，就比较事件时间
                LoginEvent lastLoginEvent = iterator.next();
                // todo 需考虑数据乱序的问题（应结合watermask）以及登录失败次数 这里是2次 若是超过2呢
                if (value.getEventTime() - lastLoginEvent.getEventTime() > 2000) {
                    // 如果两次间隔小于2s，输出报警
                    LoginWarning loginWarning = new LoginWarning();
                    loginWarning.setUserId(value.getUserId());
                    loginWarning.setFirstFailTime(lastLoginEvent.getEventTime());
                    loginWarning.setLastFailTime(value.getEventTime());
                    loginWarning.setWarningMsg("login fail in 2s");
                    out.collect(loginWarning);
                }
                // 更新最近一次的登录失败事件，保存到状态中
                loginFailListState.clear();
                loginFailListState.add(value);
            } else {
                // 之前没有登录失败事件即第一次登录失败，直接添加到状态
                loginFailListState.add(value);
            }
        } else {
            // 成功则清除状态
            loginFailListState.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, LoginEvent, LoginWarning>.OnTimerContext ctx, Collector<LoginWarning> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        //根据状态里的失败个数决定是否触发报警
        List<LoginEvent> allLoginFails = new ArrayList<>();
        for (LoginEvent loginEvent : loginFailListState.get()) {
            allLoginFails.add(loginEvent);
        }

        if (allLoginFails.size() >= maxFailNumber){
            LoginWarning loginWarning = new LoginWarning();
            loginWarning.setUserId(ctx.getCurrentKey());
            loginWarning.setFirstFailTime(allLoginFails.get(0).getEventTime());
            loginWarning.setLastFailTime(allLoginFails.get(allLoginFails.size() -1).getEventTime());
            loginWarning.setWarningMsg("login fail in 2s for " + allLoginFails.size());
            out.collect(loginWarning);
        }

        loginFailListState.clear();
    }
}
