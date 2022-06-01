package org.leave.flink.practice.login.function;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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
    /**
     * 定义属性，最大连续登录失败次数
     */
    private Integer maxFailTimes;
    /**
     * 定义状态，保存2秒内所有的登录失败事件
     */
    private ListState<LoginEvent> loginFailListState;

    /**
     * 定义状态：保存注册的定时器时间戳
     */
    ValueState<Long> timerTsState;

    public LoginWarningProcess(Integer maxFailTimes) {
        this.maxFailTimes = maxFailTimes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        loginFailListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginFailListState", LoginEvent.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
    }

    /**
     * 流中的每一个元素都会调用这个方法，调用结果将会放在Collector 数据类型中输出
     * Context可以访问元素的时间戳，元素的key,以及TimerService时间服务。Context还可以将结果输出到别的流(sideoutputs)
     */
    @Override
    public void processElement(LoginEvent value, KeyedProcessFunction<Long, LoginEvent, LoginWarning>.Context ctx, Collector<LoginWarning> out) throws Exception {
        // 判断当前登录事件类型
        if ("fail".equalsIgnoreCase(value.getEventType())) {
            // 如果是失败事件，添加到表状态中
            loginFailListState.add(value);
            // 如果没有定时器，注册一个2秒之后的定时器
            if (null ==  timerTsState.value()) {
                long ts = (value.getEventTime() + 2) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerTsState.update(ts);
            }
        } else {
            // 成功则清除状态
            if (null != timerTsState.value()) {
                ctx.timerService().deleteEventTimeTimer(timerTsState.value());
            }
            loginFailListState.clear();
            timerTsState.clear();
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
        //根据状态里的失败个数决定是否触发报警
        ArrayList<LoginEvent> allLoginFails = Lists.newArrayList(loginFailListState.get().iterator());
        int failTimes = allLoginFails.size();

        if (failTimes >= maxFailTimes) {
            LoginWarning loginWarning = new LoginWarning();
            loginWarning.setUserId(ctx.getCurrentKey());
            loginWarning.setFirstFailTime(allLoginFails.get(0).getEventTime());
            loginWarning.setLastFailTime(allLoginFails.get(allLoginFails.size() - 1).getEventTime());
            loginWarning.setWarningMsg("login fail in 2s for " + failTimes + "times");
            out.collect(loginWarning);
        }

        //清空状态
        loginFailListState.clear();
        timerTsState.clear();
    }
}
