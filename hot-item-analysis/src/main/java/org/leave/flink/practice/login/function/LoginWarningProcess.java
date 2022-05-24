package org.leave.flink.practice.login.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.login.bean.LoginEvent;
import org.leave.flink.practice.login.bean.LoginWarning;

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


    /**
     * 流中的每一个元素都会调用这个方法，调用结果将会放在Collector 数据类型中输出
     * Context可以访问元素的时间戳，元素的key,以及TimerService时间服务。Context还可以将结果输出到别的流(sideoutputs)
     */
    @Override
    public void processElement(LoginEvent value, KeyedProcessFunction<Long, LoginEvent, LoginWarning>.Context ctx, Collector<LoginWarning> out) throws Exception {
        if ("fail".equalsIgnoreCase(value.getEventType())) {
            // 如果失败，判断之前是否有登录失败事件
            Iterator<LoginEvent> iterator = loginFailListState.get().iterator();
            if (iterator.hasNext()) {
                // 如果之前已经有登录失败事件，就比较事件时间
                LoginEvent lastLoginEvent = iterator.next();
                // todo 需考虑数据乱序的问题（应结合watermark）以及登录失败次数 这里是2次 若是超过2呢
                if (value.getEventTime() - lastLoginEvent.getEventTime() < 2000) {
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
            }
            // 之前没有登录失败事件即第一次登录失败，直接添加到状态
            loginFailListState.add(value);
        } else {
            // 成功则清除状态
            loginFailListState.clear();
        }
    }

    /**
     * 回调函数
     * 当之前注册的定时器触发时调用
     * 参数timestamp为定时器所设定的触发的时间戳。Collector 为输出结果的集合。
     * OnTimerContext和processElement的Context参数一样，提供了上下文的一些信息，例如定时器触发的时间信息(事件时间或者处理时间)
     */
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, LoginEvent, LoginWarning>.OnTimerContext ctx, Collector<LoginWarning> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        //根据状态里的失败个数决定是否触发报警
        List<LoginEvent> allLoginFails = new ArrayList<>();
        for (LoginEvent loginEvent : loginFailListState.get()) {
            allLoginFails.add(loginEvent);
        }

        if (allLoginFails.size() >= maxFailNumber) {
            LoginWarning loginWarning = new LoginWarning();
            loginWarning.setUserId(ctx.getCurrentKey());
            loginWarning.setFirstFailTime(allLoginFails.get(0).getEventTime());
            loginWarning.setLastFailTime(allLoginFails.get(allLoginFails.size() - 1).getEventTime());
            loginWarning.setWarningMsg("login fail in 2s for " + allLoginFails.size());
            out.collect(loginWarning);
        }

        loginFailListState.clear();
    }
}
